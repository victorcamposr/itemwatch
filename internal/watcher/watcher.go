// Package watcher ties the pieces together: it tails the ledger, runs the
// rules, stores alerts with the checkpoint and publishes new alerts.
package watcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/victorcamposr/itemwatch/internal/detect"
	"github.com/victorcamposr/itemwatch/internal/dispatch"
	"github.com/victorcamposr/itemwatch/internal/ledger"
	"github.com/victorcamposr/itemwatch/internal/metrics"
	"github.com/victorcamposr/itemwatch/internal/queue"
	"github.com/victorcamposr/itemwatch/internal/render"
	"github.com/victorcamposr/itemwatch/internal/store"
)

// CheckpointName identifies the ledger checkpoint row.
const CheckpointName = "ledger"

// Ledger is what the watcher reads from the game database.
type Ledger interface {
	After(ctx context.Context, after uint64, limit int) ([]ledger.Event, error)
	MaxID(ctx context.Context) (uint64, error)
	LastIDBefore(ctx context.Context, t time.Time) (uint64, error)
	Duplicates(ctx context.Context, limit int) ([]ledger.Duplicate, error)
	PlayerNames(ctx context.Context, ids []int64) (map[int64]string, error)
}

// Publisher appends notifications to the stream.
type Publisher interface {
	Publish(ctx context.Context, n queue.Notification) (string, error)
}

// Watcher is the leader-only part of the service.
type Watcher struct {
	Ledger       Ledger
	Store        *store.Store
	Publisher    Publisher
	Rules        []detect.Rule
	Dupes        *detect.DupeTracker
	Lang         render.Lang
	BatchSize    int
	PollInterval time.Duration
	DupeInterval time.Duration
	StartLatest  bool // with no checkpoint, skip the history instead of replaying it
	Log          *slog.Logger
	Metrics      *metrics.Metrics
	Direct       []dispatch.Notifier // channels for what cannot wait for the queue

	publishMu sync.Mutex

	// What /v1/status reads while the loops run.
	tailing  atomic.Bool
	cursor   atomic.Uint64
	lastRead atomic.Int64 // unix milliseconds, 0 before the first read
	lastDupe atomic.Int64 // unix milliseconds, 0 before the first scan
}

// State is a snapshot of the running watcher, safe to take from any goroutine.
type State struct {
	Tailing    bool // this replica is reading the ledger, so Cursor is live
	Cursor     uint64
	LastReadAt *time.Time // last successful poll, even if it found nothing
	DupeScan   bool
	DupeScanAt *time.Time // last scan that completed
}

// State returns the current state.
func (w *Watcher) State() State {
	return State{
		Tailing:    w.tailing.Load(),
		Cursor:     w.cursor.Load(),
		LastReadAt: fromMillis(w.lastRead.Load()),
		DupeScan:   w.Dupes != nil,
		DupeScanAt: fromMillis(w.lastDupe.Load()),
	}
}

func fromMillis(ms int64) *time.Time {
	if ms == 0 {
		return nil
	}
	t := time.UnixMilli(ms).UTC()
	return &t
}

// Run processes until ctx ends.
func (w *Watcher) Run(ctx context.Context) error {
	cursor, err := w.restore(ctx)
	if err != nil {
		return err
	}
	if cursor, err = w.FollowReset(ctx, cursor); err != nil {
		return err
	}
	w.Log.Info("watching ledger", "cursor", cursor, "rules", len(w.Rules))

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return w.tail(ctx, cursor) })
	if w.Dupes != nil {
		g.Go(func() error { return w.scanDupes(ctx) })
	}
	g.Go(func() error { return w.republish(ctx) })
	return g.Wait()
}

func (w *Watcher) restore(ctx context.Context) (uint64, error) {
	cp, ok, err := w.Store.LoadCheckpoint(ctx, CheckpointName)
	if err != nil {
		return 0, err
	}
	if !ok {
		for _, rule := range w.Rules {
			if s, ok := rule.(detect.Stateful); ok {
				_ = s.LoadState(json.RawMessage("{}"))
			}
		}
		if !w.StartLatest {
			return 0, nil
		}
		latest, err := w.Ledger.MaxID(ctx)
		if err != nil {
			return 0, err
		}
		// Persist the starting point so a crash before the first batch does
		// not skip whatever arrived in between.
		empty := &store.Checkpoint{Cursor: latest, RuleState: map[string]json.RawMessage{}}
		if err := w.Store.Commit(ctx, CheckpointName, empty, nil); err != nil {
			return 0, err
		}
		return latest, nil
	}
	for _, rule := range w.Rules {
		s, ok := rule.(detect.Stateful)
		if !ok {
			continue
		}
		raw, saved := cp.RuleState[rule.Name()]
		if !saved {
			raw = json.RawMessage("{}")
		}
		if err := s.LoadState(raw); err != nil {
			w.Log.Warn("rule state discarded", "rule", rule.Name(), "err", err)
		}
	}
	return cp.Cursor, nil
}

func (w *Watcher) tail(ctx context.Context, cursor uint64) error {
	w.cursor.Store(cursor)
	w.tailing.Store(true)
	defer w.tailing.Store(false)

	lagTick := time.NewTicker(15 * time.Second)
	defer lagTick.Stop()
	for ctx.Err() == nil {
		select {
		case <-lagTick.C:
			if max, err := w.Ledger.MaxID(ctx); err == nil && max >= cursor {
				w.Metrics.CursorLag.Set(float64(max - cursor))
			} else if err == nil {
				next, err := w.FollowReset(ctx, cursor)
				if err != nil {
					w.Log.Error("ledger reset check failed", "cursor", cursor, "err", err)
				} else {
					cursor = next
					w.cursor.Store(cursor)
				}
			}
		default:
		}

		next, n, err := w.Step(ctx, cursor)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			w.Log.Error("ledger batch failed", "cursor", cursor, "err", err)
			sleep(ctx, 5*time.Second)
			if errors.Is(err, errUncommitted) {
				// The rules already saw this batch in memory: rewind them to the
				// checkpoint so the retry does not count it twice.
				restored, ok := w.rewind(ctx)
				if !ok {
					break
				}
				cursor = restored
				w.cursor.Store(cursor)
			}
			continue
		}
		cursor = next
		w.cursor.Store(cursor)
		if n < w.BatchSize {
			sleep(ctx, w.PollInterval)
		}
	}
	return nil
}

// resetMargin is how far before the last checkpoint FollowReset resumes. The
// checkpoint is written right after its batch, so rows a few seconds older are
// already counted; reading them again costs less than skipping new ones.
const resetMargin = 5 * time.Second

// FollowReset handles a ledger whose newest id is below the cursor. That only
// happens when the table was rebuilt or emptied and its ids started over
// lower: every new row would then stay below the cursor and never be read,
// while the lag still reads zero. It moves the cursor back to the last row
// created before the checkpoint and stores a warning alert in the same
// commit, so the team knows the history was cut. It returns the cursor to
// use, unchanged when the ledger is fine.
func (w *Watcher) FollowReset(ctx context.Context, cursor uint64) (uint64, error) {
	head, err := w.Ledger.MaxID(ctx)
	if err != nil || head >= cursor {
		return cursor, err
	}
	cp, ok, err := w.Store.LoadCheckpoint(ctx, CheckpointName)
	if err != nil || !ok {
		return cursor, err
	}
	target, err := w.Ledger.LastIDBefore(ctx, cp.UpdatedAt.Add(-resetMargin))
	if err != nil {
		return cursor, err
	}
	target = min(target, head)
	now := time.Now().UTC()
	finding := detect.Finding{
		Rule:     detect.LedgerResetRule,
		Severity: detect.Warning,
		Key:      detect.LedgerResetKey(cursor),
		At:       now,
		Count:    1,
		Origin:   "ledger",
		Facts:    map[string]any{"cursor": cursor, "head": head, "resume_from": target, "checkpoint_at": cp.UpdatedAt.UTC().Format(time.RFC3339)},
	}
	alerts, err := w.toAlerts(ctx, []detect.Finding{finding})
	if err != nil {
		return cursor, err
	}
	if err := w.Store.Commit(ctx, CheckpointName, &store.Checkpoint{Cursor: target, RuleState: cp.RuleState}, alerts); err != nil {
		return cursor, err
	}
	w.Log.Warn("ledger ids went back: resuming below the old cursor", "cursor", cursor, "head", head, "resume_from", target)
	w.PublishPending(ctx)
	return target, nil
}

// rewind restores the checkpoint, waiting for as long as the state database
// is out. Returning an error here would stop the whole process for an outage
// the leader lock already handles. ok is false when ctx ended first.
func (w *Watcher) rewind(ctx context.Context) (uint64, bool) {
	for {
		cursor, err := w.restore(ctx)
		if err == nil {
			return cursor, true
		}
		if ctx.Err() != nil {
			return 0, false
		}
		w.Log.Error("restore checkpoint failed", "err", err)
		sleep(ctx, 5*time.Second)
	}
}

// Step processes one batch after cursor and returns the new cursor and the
// number of events read. Rule state advances in memory first; if the commit
// fails, Step returns errUncommitted and the caller restores the checkpoint,
// bringing cursor and state back to the same point.
func (w *Watcher) Step(ctx context.Context, cursor uint64) (uint64, int, error) {
	start := time.Now()
	events, err := w.Ledger.After(ctx, cursor, w.BatchSize)
	if err != nil {
		return cursor, 0, err
	}
	w.lastRead.Store(time.Now().UnixMilli())
	if len(events) == 0 {
		return cursor, 0, nil
	}
	var findings []detect.Finding
	for _, ev := range events {
		for _, rule := range w.Rules {
			findings = append(findings, rule.Observe(ev)...)
		}
	}
	last := events[len(events)-1].ID

	state := make(map[string]json.RawMessage, len(w.Rules))
	for _, rule := range w.Rules {
		if s, ok := rule.(detect.Stateful); ok {
			raw, err := s.SaveState()
			if err != nil {
				return cursor, 0, fmt.Errorf("save %s state: %w", rule.Name(), err)
			}
			state[rule.Name()] = raw
		}
	}
	alerts, err := w.toAlerts(ctx, findings)
	if err != nil {
		return cursor, 0, fmt.Errorf("%w: %v", errUncommitted, err)
	}
	if err := w.Store.Commit(ctx, CheckpointName, &store.Checkpoint{Cursor: last, RuleState: state}, alerts); err != nil {
		return cursor, 0, fmt.Errorf("%w: %v", errUncommitted, err)
	}
	w.Metrics.RowsProcessed.Add(float64(len(events)))
	w.Metrics.BatchDuration.Observe(time.Since(start).Seconds())
	for _, f := range findings {
		w.Metrics.Findings.WithLabelValues(f.Rule, f.Severity.String()).Inc()
	}
	if len(alerts) > 0 {
		w.PublishPending(ctx)
	}
	return last, len(events), nil
}

var errUncommitted = errors.New("batch evaluated but not committed")

func (w *Watcher) scanDupes(ctx context.Context) error {
	t := time.NewTicker(w.DupeInterval)
	defer t.Stop()
	for {
		if err := w.ScanDupesOnce(ctx); err == nil {
			w.lastDupe.Store(time.Now().UnixMilli())
		} else if ctx.Err() == nil {
			w.Log.Error("duplicate scan failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// ScanDupesOnce runs one duplicate scan.
func (w *Watcher) ScanDupesOnce(ctx context.Context) error {
	start := time.Now()
	dups, err := w.Ledger.Duplicates(ctx, 500)
	if err != nil {
		return err
	}
	findings := w.Dupes.Scan(time.Now().UTC(), dups)
	w.Metrics.DupeScanDuration.Observe(time.Since(start).Seconds())
	w.Metrics.DupesTracked.Set(float64(w.Dupes.Tracked()))
	if len(findings) == 0 {
		return nil
	}
	alerts, err := w.toAlerts(ctx, findings)
	if err != nil {
		return err
	}
	if err := w.Store.Commit(ctx, CheckpointName, nil, alerts); err != nil {
		return err
	}
	for _, f := range findings {
		w.Metrics.Findings.WithLabelValues(f.Rule, f.Severity.String()).Inc()
	}
	w.PublishPending(ctx)
	return nil
}

// toAlerts resolves player names and renders the findings.
func (w *Watcher) toAlerts(ctx context.Context, findings []detect.Finding) ([]store.NewAlert, error) {
	if len(findings) == 0 {
		return nil, nil
	}
	ids := map[int64]bool{}
	for _, f := range findings {
		if f.PlayerID > 0 {
			ids[f.PlayerID] = true
		}
		if holders, ok := f.Facts["holders"].([]ledger.Holder); ok {
			for _, h := range holders {
				ids[h.PlayerID] = true
			}
		}
	}
	list := make([]int64, 0, len(ids))
	for id := range ids {
		list = append(list, id)
	}
	names, err := w.Ledger.PlayerNames(ctx, list)
	if err != nil {
		return nil, err
	}

	alerts := make([]store.NewAlert, 0, len(findings))
	for _, f := range findings {
		facts := f.Facts
		if holders, ok := facts["holders"].([]ledger.Holder); ok {
			named := make([]map[string]any, len(holders))
			for i, h := range holders {
				named[i] = map[string]any{"place": h.Place, "player_id": h.PlayerID, "player_name": names[h.PlayerID]}
			}
			facts = copyFacts(facts)
			facts["holders"] = named
		}
		details, err := json.Marshal(facts)
		if err != nil {
			return nil, err
		}
		alerts = append(alerts, store.NewAlert{
			DedupKey:   f.Key,
			Rule:       f.Rule,
			Severity:   int8(f.Severity),
			Title:      render.Title(w.Lang, f),
			Details:    details,
			PlayerID:   f.PlayerID,
			PlayerName: names[f.PlayerID],
			ItemHash:   f.ItemHash,
			Origin:     f.Origin,
			Count:      max(f.Count, 1),
			At:         f.At,
		})
	}
	return alerts, nil
}

func copyFacts(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// republish is the recovery half of the dual write: anything committed but
// not marked published (crash, Redis down) is pushed again.
func (w *Watcher) republish(ctx context.Context) error {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		w.PublishPending(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// PublishPending pushes unpublished alerts to the stream.
func (w *Watcher) PublishPending(ctx context.Context) {
	w.publishMu.Lock()
	defer w.publishMu.Unlock()
	for ctx.Err() == nil {
		alerts, err := w.Store.Unpublished(ctx, 100)
		if err != nil {
			w.Log.Warn("list unpublished", "err", err)
			return
		}
		for _, a := range alerts {
			_, err := w.Publisher.Publish(ctx, queue.Notification{
				AlertID: a.ID, Generation: a.Generation, Rule: a.Rule, Severity: a.Severity, Title: a.Title,
				PlayerName: a.PlayerName, Origin: a.Origin, ItemHash: a.ItemHash,
				Occurrences: a.Occurrences, FirstSeen: a.FirstSeen,
			})
			if err != nil {
				w.Log.Warn("publish alert", "alert", a.ID, "err", err)
				return
			}
			if err := w.Store.MarkPublished(ctx, a.ID, a.Generation); err != nil {
				w.Log.Warn("mark published", "alert", a.ID, "err", err)
				return
			}
			w.Metrics.Published.Inc()
		}
		if len(alerts) < 100 {
			return
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
