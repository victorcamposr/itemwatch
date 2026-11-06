package watcher

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/victorcamposr/itemwatch/internal/detect"
	"github.com/victorcamposr/itemwatch/internal/ledger"
	"github.com/victorcamposr/itemwatch/internal/metrics"
)

type emptyLedger struct{ Ledger }

func (emptyLedger) After(context.Context, uint64, int) ([]ledger.Event, error) { return nil, nil }
func (emptyLedger) MaxID(context.Context) (uint64, error)                      { return 5, nil }

func TestStateFollowsTheTail(t *testing.T) {
	w := &Watcher{
		Ledger: emptyLedger{}, BatchSize: 10, PollInterval: time.Millisecond,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: metrics.New(),
	}
	if s := w.State(); s.Tailing || s.LastReadAt != nil || s.DupeScan || s.DupeScanAt != nil {
		t.Fatalf("before start: %+v", s)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.tail(ctx, 5) }()

	deadline := time.Now().Add(2 * time.Second)
	for w.State().LastReadAt == nil {
		if time.Now().After(deadline) {
			t.Fatal("no read recorded")
		}
		time.Sleep(time.Millisecond)
	}
	// An empty poll still counts: an idle server must not look stuck.
	if s := w.State(); !s.Tailing || s.Cursor != 5 {
		t.Fatalf("while tailing: %+v", s)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if s := w.State(); s.Tailing || s.LastReadAt == nil {
		t.Fatalf("after losing the lead: %+v", s)
	}
}

func TestStateReportsTheDupeScan(t *testing.T) {
	w := &Watcher{Dupes: detect.NewDupeTracker(time.Minute)}
	if !w.State().DupeScan {
		t.Fatal("dupe scan not reported as enabled")
	}
}
