package api

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/victorcamposr/itemwatch/internal/probe"
	"github.com/victorcamposr/itemwatch/internal/watcher"
)

// statusTimeout bounds each dependency of the status route, so one that hangs
// costs that long and no more.
const statusTimeout = 2 * time.Second

// unknown is the value of a number whose source did not answer.
const unknown = -1

// Runtime feeds GET /v1/status. Each dependency can fail on its own: its fields
// turn to -1 and the route still answers 200, because a broken dependency is
// exactly when someone reads this. All functions must be safe for concurrent
// use.
type Runtime struct {
	Version   string
	StartedAt time.Time
	Leader    func() bool
	Watcher   func() watcher.State
	Cursor    func(ctx context.Context) (uint64, error) // saved checkpoint, for a replica not tailing
	Head      func(ctx context.Context) (uint64, error)
	Queue     func(ctx context.Context) (pending, deadLetter int64) // -1 when unknown
	Probes    func() []probe.Status
}

// LedgerStatus is how far behind the ledger the service is.
type LedgerStatus struct {
	Cursor     int64      `json:"cursor"`
	Head       int64      `json:"head"`
	Lag        int64      `json:"lag"`
	LastReadAt *time.Time `json:"last_read_at"`
}

// DupeScanStatus is the state of the duplicate scan.
type DupeScanStatus struct {
	Enabled bool       `json:"enabled"`
	LastAt  *time.Time `json:"last_at"`
}

// QueueStatus is what is left to deliver.
type QueueStatus struct {
	Pending    int64 `json:"pending"`
	DeadLetter int64 `json:"dead_letter"`
}

// Status is the body of GET /v1/status.
type Status struct {
	Version   string         `json:"version"`
	StartedAt time.Time      `json:"started_at"`
	Leader    bool           `json:"leader"`
	Ledger    LedgerStatus   `json:"ledger"`
	DupeScan  DupeScanStatus `json:"dupe_scan"`
	Queue     QueueStatus    `json:"queue"`
	Probes    []probe.Status `json:"probes"`
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.Runtime.report(r.Context()))
}

func (rt *Runtime) report(ctx context.Context) Status {
	state := rt.Watcher()
	st := Status{
		Version:   rt.Version,
		StartedAt: rt.StartedAt,
		Leader:    rt.Leader(),
		Ledger:    LedgerStatus{Cursor: int64(state.Cursor), Head: unknown, Lag: unknown, LastReadAt: state.LastReadAt},
		DupeScan:  DupeScanStatus{Enabled: state.DupeScan, LastAt: state.DupeScanAt},
		Queue:     QueueStatus{Pending: unknown, DeadLetter: unknown},
		Probes:    rt.Probes(),
	}

	// Dependencies are asked in parallel: with one down, the others still
	// answer inside the timeout.
	ctx, cancel := context.WithTimeout(ctx, statusTimeout)
	defer cancel()
	var wg sync.WaitGroup
	if !state.Tailing {
		st.Ledger.Cursor = unknown
		wg.Go(func() {
			if cursor, err := rt.Cursor(ctx); err == nil {
				st.Ledger.Cursor = int64(cursor)
			}
		})
	}
	wg.Go(func() {
		if head, err := rt.Head(ctx); err == nil {
			st.Ledger.Head = int64(head)
		}
	})
	wg.Go(func() { st.Queue.Pending, st.Queue.DeadLetter = rt.Queue(ctx) })
	wg.Wait()

	if st.Ledger.Cursor != unknown && st.Ledger.Head != unknown {
		st.Ledger.Lag = max(st.Ledger.Head-st.Ledger.Cursor, 0)
	}
	return st
}
