package probe

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

type fakeCheck struct {
	name string
	err  error
	runs int
}

func (c *fakeCheck) Name() string { return c.name }
func (c *fakeCheck) Run(context.Context) (map[string]any, error) {
	c.runs++
	return map[string]any{"n": c.runs}, c.err
}

type fakeAlerts struct {
	raised  []Incident
	cleared []string
	direct  []Incident

	raiseErr, clearErr, notifyErr error
	raiseCalls                    int
}

func (f *fakeAlerts) RaiseProbe(_ context.Context, inc Incident) error {
	f.raiseCalls++
	if f.raiseErr != nil {
		return f.raiseErr
	}
	f.raised = append(f.raised, inc)
	return nil
}

func (f *fakeAlerts) ClearProbe(_ context.Context, check string) error {
	if f.clearErr != nil {
		return f.clearErr
	}
	f.cleared = append(f.cleared, check)
	return nil
}

func (f *fakeAlerts) NotifyProbe(_ context.Context, inc Incident) error {
	if f.notifyErr != nil {
		return f.notifyErr
	}
	f.direct = append(f.direct, inc)
	return nil
}

type rig struct {
	check  *fakeCheck
	alerts *fakeAlerts
	runner *Runner
	now    time.Time
	active bool
}

func newRig(t *testing.T) *rig {
	t.Helper()
	r := &rig{
		check:  &fakeCheck{name: "ot-status"},
		alerts: &fakeAlerts{},
		now:    time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC),
		active: true,
	}
	r.runner = New(Config{
		Checks: []Check{r.check}, Alerts: r.alerts, FailAfter: 5 * time.Minute,
		Active: func(context.Context) bool { return r.active },
		Now:    func() time.Time { return r.now },
	})
	return r
}

// tick advances the clock and runs one tick.
func (r *rig) tick(d time.Duration) Status {
	r.now = r.now.Add(d)
	r.runner.Tick(context.Background())
	if s := r.runner.Snapshot(); len(s) > 0 {
		return s[0]
	}
	return Status{}
}

func (r *rig) settle(t *testing.T) {
	t.Helper()
	r.check.err = nil
	r.tick(time.Minute)
	r.alerts.cleared, r.alerts.raised, r.alerts.direct = nil, nil, nil
}

func TestOKThenFailThenRecover(t *testing.T) {
	r := newRig(t)
	s := r.tick(0)
	if !s.OK || s.LastOKAt == nil || s.FailingSince != nil || s.Alert != AlertNone {
		t.Fatalf("first success: %+v", s)
	}
	r.alerts.cleared = nil

	r.check.err = errors.New("connection refused")
	start := r.now.Add(time.Minute)
	s = r.tick(time.Minute)
	if s.OK || s.FailingSince == nil || !s.FailingSince.Equal(start) || s.LastError != "connection refused" {
		t.Fatalf("first failure: %+v", s)
	}
	if s.Facts != nil {
		t.Fatalf("facts of the last success kept: %+v", s)
	}

	// A planned restart: still inside FailAfter, nothing is raised.
	r.tick(4 * time.Minute)
	if s = r.tick(59 * time.Second); len(r.alerts.raised) != 0 || s.Alert != AlertNone {
		t.Fatalf("raised inside FailAfter: %+v", r.alerts.raised)
	}

	s = r.tick(time.Second)
	if len(r.alerts.raised) != 1 || s.Alert != AlertStored {
		t.Fatalf("not raised at FailAfter: %+v %+v", r.alerts.raised, s)
	}
	inc := r.alerts.raised[0]
	if inc.Check != "ot-status" || !inc.Since.Equal(start) || inc.Error != "connection refused" || inc.Quiet {
		t.Fatalf("incident %+v", inc)
	}

	r.tick(time.Minute)
	r.tick(time.Minute)
	if len(r.alerts.raised) != 1 {
		t.Fatalf("raised %d times for one incident", len(r.alerts.raised))
	}

	r.check.err = nil
	s = r.tick(time.Minute)
	if !slices.Equal(r.alerts.cleared, []string{"ot-status"}) {
		t.Fatalf("not cleared: %v", r.alerts.cleared)
	}
	if !s.OK || s.FailingSince != nil || s.LastError != "" || s.Alert != AlertNone {
		t.Fatalf("recovered: %+v", s)
	}
	r.tick(time.Minute)
	if len(r.alerts.cleared) != 1 {
		t.Fatalf("cleared %d times", len(r.alerts.cleared))
	}
}

func TestFailingAgainRaisesAgain(t *testing.T) {
	r := newRig(t)
	r.settle(t)
	r.check.err = errors.New("down")
	r.tick(time.Minute)
	r.tick(5 * time.Minute)
	r.check.err = nil
	r.tick(time.Minute)

	r.check.err = errors.New("down again")
	r.tick(time.Minute)
	if len(r.alerts.raised) != 1 {
		t.Fatalf("FailAfter must count again: %+v", r.alerts.raised)
	}
	r.tick(5 * time.Minute)
	if len(r.alerts.raised) != 2 || r.alerts.raised[1].Error != "down again" {
		t.Fatalf("second incident: %+v", r.alerts.raised)
	}
}

func TestShortFailuresDoNotAddUp(t *testing.T) {
	r := newRig(t)
	r.settle(t)
	for range 5 {
		r.check.err = errors.New("down")
		r.tick(time.Minute)
		r.tick(3 * time.Minute)
		r.check.err = nil
		r.tick(time.Minute)
	}
	if len(r.alerts.raised) != 0 {
		t.Fatalf("raised %+v", r.alerts.raised)
	}
}

// An alert left open by a previous process must be closed by the first
// success, even though this process never saw the failure.
func TestFirstSuccessClearsAlertFromEarlierRun(t *testing.T) {
	r := newRig(t)
	r.alerts.clearErr = errors.New("database down")
	r.tick(time.Minute)
	if len(r.alerts.cleared) != 0 {
		t.Fatal("cleared with an error")
	}
	r.alerts.clearErr = nil
	r.tick(time.Minute)
	r.tick(time.Minute)
	if len(r.alerts.cleared) != 1 {
		t.Fatalf("cleared %d times, want one retry that sticks", len(r.alerts.cleared))
	}
}

func TestRefusedAlertGoesDirectOnceThenBackfills(t *testing.T) {
	r := newRig(t)
	r.settle(t)
	refused := errors.New("read only")
	r.check.err = errors.New("insert denied")
	r.alerts.raiseErr = refused
	start := r.now.Add(time.Minute)
	r.tick(time.Minute)

	s := r.tick(5 * time.Minute)
	if s.Alert != AlertUndelivered || len(r.alerts.direct) != 0 {
		t.Fatalf("one retry comes before going direct: %+v %+v", s, r.alerts.direct)
	}
	s = r.tick(time.Minute)
	if s.Alert != AlertDirect || len(r.alerts.direct) != 1 {
		t.Fatalf("not announced directly: %+v %+v", s, r.alerts.direct)
	}
	if inc := r.alerts.direct[0]; !inc.Since.Equal(start) || inc.Check != "ot-status" {
		t.Fatalf("direct incident %+v", inc)
	}

	calls := r.alerts.raiseCalls
	r.tick(time.Minute)
	r.tick(time.Minute)
	if len(r.alerts.direct) != 1 {
		t.Fatalf("announced %d times", len(r.alerts.direct))
	}
	if r.alerts.raiseCalls != calls+2 {
		t.Fatal("storing must keep being tried")
	}

	r.check.err, r.alerts.raiseErr = nil, nil
	s = r.tick(time.Minute)
	if len(r.alerts.raised) != 1 || !r.alerts.raised[0].Quiet || !r.alerts.raised[0].Since.Equal(start) {
		t.Fatalf("history row: %+v", r.alerts.raised)
	}
	if r.alerts.raised[0].Error != "insert denied" {
		t.Fatalf("history lost the error: %+v", r.alerts.raised[0])
	}
	if len(r.alerts.cleared) != 1 || s.Alert != AlertNone {
		t.Fatalf("not resolved: %v %+v", r.alerts.cleared, s)
	}
}

// The database came back while the check is still failing: the row appears,
// but the channels already heard about this incident.
func TestDirectAlertStoredQuietlyWhileStillFailing(t *testing.T) {
	r := newRig(t)
	r.settle(t)
	r.check.err = errors.New("down")
	r.alerts.raiseErr = errors.New("read only")
	r.tick(time.Minute)
	r.tick(5 * time.Minute)
	r.tick(time.Minute)

	r.alerts.raiseErr = nil
	s := r.tick(time.Minute)
	if len(r.alerts.raised) != 1 || !r.alerts.raised[0].Quiet || s.Alert != AlertStored {
		t.Fatalf("%+v %+v", r.alerts.raised, s)
	}
	r.tick(time.Minute)
	if len(r.alerts.raised) != 1 {
		t.Fatal("stored twice")
	}
}

func TestBackfillWaitsForTheDatabase(t *testing.T) {
	r := newRig(t)
	r.settle(t)
	r.check.err = errors.New("down")
	r.alerts.raiseErr = errors.New("read only")
	r.tick(time.Minute)
	r.tick(5 * time.Minute)
	r.tick(time.Minute)

	r.check.err = nil
	if s := r.tick(time.Minute); !s.OK || s.Alert != AlertDirect {
		t.Fatalf("recovered check with the row still pending: %+v", s)
	}
	r.alerts.raiseErr = nil
	s := r.tick(time.Minute)
	if len(r.alerts.raised) != 1 || !r.alerts.raised[0].Quiet || s.Alert != AlertNone {
		t.Fatalf("%+v %+v", r.alerts.raised, s)
	}
}

func TestUndeliveredIsReportedAndRetried(t *testing.T) {
	r := newRig(t)
	r.settle(t)
	r.check.err = errors.New("down")
	r.alerts.raiseErr = errors.New("read only")
	r.alerts.notifyErr = errors.New("webhook down")
	r.tick(time.Minute)
	r.tick(5 * time.Minute)
	s := r.tick(time.Minute)
	if s.Alert != AlertUndelivered {
		t.Fatalf("status must show nobody heard: %+v", s)
	}

	r.alerts.notifyErr = nil
	s = r.tick(time.Minute)
	if s.Alert != AlertDirect || len(r.alerts.direct) != 1 {
		t.Fatalf("notify not retried: %+v", s)
	}
}

func TestStandbyDoesNotRun(t *testing.T) {
	r := newRig(t)
	r.active = false
	r.tick(time.Minute)
	if r.check.runs != 0 {
		t.Fatal("ran on a standby")
	}
	if s := r.runner.Snapshot(); len(s) != 0 {
		t.Fatalf("snapshot %+v", s)
	}
}

func TestLockBlipKeepsTheIncident(t *testing.T) {
	r := newRig(t)
	r.settle(t)
	r.check.err = errors.New("down")
	start := r.now.Add(time.Minute)
	r.tick(time.Minute)

	r.active = false
	r.tick(20 * time.Second)
	r.tick(20 * time.Second)
	r.active = true
	s := r.tick(20 * time.Second)
	if s.FailingSince == nil || !s.FailingSince.Equal(start) {
		t.Fatalf("incident forgotten: %+v", s)
	}
}

func TestLongStandbyForgets(t *testing.T) {
	r := newRig(t)
	r.settle(t)
	r.check.err = errors.New("down")
	r.tick(time.Minute)

	r.active = false
	r.tick(time.Minute)
	r.tick(time.Minute)
	if s := r.runner.Snapshot(); len(s) != 0 {
		t.Fatalf("stale state kept: %+v", s)
	}

	r.active = true
	r.tick(time.Minute)
	if len(r.alerts.raised) != 0 {
		t.Fatal("old failing_since carried over")
	}
}

func TestShutdownIsNotAFailure(t *testing.T) {
	r := newRig(t)
	r.settle(t)
	r.check.err = context.Canceled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r.runner.Tick(ctx)
	if s := r.runner.Snapshot(); !s[0].OK {
		t.Fatalf("a canceled tick counted as a failure: %+v", s[0])
	}
}

type hangingCheck struct{}

func (hangingCheck) Name() string { return "hang" }
func (hangingCheck) Run(ctx context.Context) (map[string]any, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestHungCheckDoesNotBlockTheOthers(t *testing.T) {
	ok := &fakeCheck{name: "ok"}
	r := New(Config{Checks: []Check{hangingCheck{}, ok}, Alerts: &fakeAlerts{}, Timeout: 20 * time.Millisecond})
	r.Tick(context.Background())
	s := r.Snapshot()
	if len(s) != 2 || s[0].OK || !s[1].OK {
		t.Fatalf("%+v", s)
	}
	if s[0].LastError == "" {
		t.Fatal("timeout not reported")
	}
}

func TestRunKeepsTickingAndWakes(t *testing.T) {
	c := &lockedCheck{}
	r := New(Config{Checks: []Check{c}, Alerts: &fakeAlerts{}, Interval: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	wait := func(want int) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for c.count() < want {
			if time.Now().After(deadline) {
				t.Fatalf("runs %d, want %d", c.count(), want)
			}
			time.Sleep(time.Millisecond)
		}
	}
	wait(1)
	r.Wake()
	wait(2)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned %v", err)
	}
}

type lockedCheck struct {
	mu   sync.Mutex
	runs int
}

func (c *lockedCheck) Name() string { return "locked" }
func (c *lockedCheck) Run(context.Context) (map[string]any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.runs++
	return nil, nil
}

func (c *lockedCheck) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.runs
}

func TestRunWithoutChecksReturns(t *testing.T) {
	if err := New(Config{}).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotIsSafeDuringTicks(t *testing.T) {
	r := New(Config{Checks: []Check{&lockedCheck{}}, Alerts: &fakeAlerts{}})
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 200 {
			r.Tick(context.Background())
		}
	})
	wg.Go(func() {
		for range 200 {
			r.Snapshot()
		}
	})
	wg.Wait()
}
