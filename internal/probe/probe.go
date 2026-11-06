// Package probe runs synthetic checks against the game stack and turns the
// ones that stay broken into alerts.
//
// A failed check is not an incident yet: a planned restart fails every check
// for a while. The alert is raised only after FailAfter of continuous failure
// and resolved by the first success. This state lives in memory, so restarting
// itemwatch restarts the FailAfter clock; an alert left open by a previous run
// is closed by the check's first success.
//
// The runner lives for the whole process and asks Active on every tick. See
// Config.Active for why it is not simply started by the leader.
package probe

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Check is one synthetic test. Run must honour ctx. The returned facts are
// optional measurements shown on /v1/status.
type Check interface {
	Name() string
	Run(ctx context.Context) (map[string]any, error)
}

// Incident is a check past FailAfter.
type Incident struct {
	Check string
	Since time.Time
	Error string
	Quiet bool // store it without going through the queue
}

// Alerts is the way out of the runner.
type Alerts interface {
	// RaiseProbe stores the incident and queues its announcement, unless
	// Quiet is set. It fails when the state database cannot take the write.
	RaiseProbe(ctx context.Context, inc Incident) error
	// ClearProbe resolves the check's open alert, if there is one.
	ClearProbe(ctx context.Context, check string) error
	// NotifyProbe announces the incident straight on the notification
	// channels, using neither the state database nor the queue.
	NotifyProbe(ctx context.Context, inc Incident) error
}

// Where an incident stands, as shown on /v1/status.
const (
	// AlertNone means there is nothing to report for this check.
	AlertNone = "none"
	// AlertStored means the alert is in the database, on the normal path.
	AlertStored = "stored"
	// AlertDirect means the alert could not be stored and was announced
	// straight on the channels. Its row is written once the database is back.
	AlertDirect = "direct"
	// AlertUndelivered means the check is past FailAfter and the alert reached
	// nobody: it exists only in the error log and on /v1/status.
	AlertUndelivered = "undelivered"
)

// directAfter is how many failed attempts to store an alert come before
// announcing it directly: one retry, a tick later, absorbs a passing error.
const directAfter = 2

// standbyGrace is how long a replica waits to be active again before dropping
// its probe state. It outlasts the leader lock retry, so a lock lost and taken
// back does not forget the incidents in progress.
const standbyGrace = time.Minute

// Status is one check as reported by /v1/status.
type Status struct {
	Name         string         `json:"name"`
	OK           bool           `json:"ok"`
	FailingSince *time.Time     `json:"failing_since"`
	LastOKAt     *time.Time     `json:"last_ok_at"`
	LastError    string         `json:"last_error"`
	MS           int64          `json:"ms"`
	Alert        string         `json:"alert"`
	Facts        map[string]any `json:"facts,omitempty"`
}

// Config sets up a Runner.
type Config struct {
	Checks    []Check
	Alerts    Alerts
	Interval  time.Duration // between runs, default 30s
	FailAfter time.Duration // continuous failure before alerting, default 5m
	Timeout   time.Duration // for each check and each alert call, default 5s

	// Active tells whether this replica should run the checks now. The service
	// answers true on the leader, and also on any replica that cannot reach
	// the state database: the leader lock lives there, so a dead database
	// means no leader, and leader-only probes would go quiet in the very
	// outage they exist to report. Nil means always active.
	Active func(ctx context.Context) bool

	Now func() time.Time
	Log *slog.Logger
}

// state is written on the Tick goroutine only. The mutex covers what Snapshot
// reads; incident, stale and attempts stay on the Tick goroutine.
type state struct {
	failingSince time.Time
	lastOK       time.Time
	lastError    string
	ms           int64
	facts        map[string]any
	alert        string

	incident Incident // the latest failure, kept after the check recovers
	stale    bool     // an open alert from an earlier run may exist
	attempts int      // failed attempts to store the current incident
}

// Runner runs the checks on a schedule.
type Runner struct {
	cfg  Config
	wake chan struct{}

	mu           sync.Mutex
	states       map[string]*state
	standbySince time.Time
}

// New returns a Runner.
func New(cfg Config) *Runner {
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.FailAfter <= 0 {
		cfg.FailAfter = 5 * time.Minute
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.DiscardHandler)
	}
	return &Runner{cfg: cfg, wake: make(chan struct{}, 1), states: map[string]*state{}}
}

// Run ticks until ctx ends. It never returns an error: a probe problem must
// not stop the watcher.
func (r *Runner) Run(ctx context.Context) error {
	if len(r.cfg.Checks) == 0 {
		return nil
	}
	t := time.NewTicker(r.cfg.Interval)
	defer t.Stop()
	for {
		r.Tick(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		case <-r.wake:
		}
	}
}

// Wake runs a tick now instead of waiting for the interval.
func (r *Runner) Wake() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

type result struct {
	facts map[string]any
	err   error
	ms    int64
}

// Tick runs every check once and acts on the results. Checks run in parallel,
// so one that hangs until its timeout does not delay the others; the results
// are applied one by one afterwards.
func (r *Runner) Tick(ctx context.Context) {
	if !r.active(ctx) {
		r.standby()
		return
	}
	r.mu.Lock()
	r.standbySince = time.Time{}
	r.mu.Unlock()

	results := make([]result, len(r.cfg.Checks))
	var wg sync.WaitGroup
	for i, c := range r.cfg.Checks {
		wg.Go(func() { results[i] = r.run(ctx, c) })
	}
	wg.Wait()
	if ctx.Err() != nil {
		return // cut short by a shutdown, which says nothing about the checks
	}
	for i, c := range r.cfg.Checks {
		r.apply(ctx, c.Name(), results[i])
	}
}

func (r *Runner) active(ctx context.Context) bool {
	if r.cfg.Active == nil {
		return true
	}
	ctx, cancel := context.WithTimeout(ctx, r.cfg.Timeout)
	defer cancel()
	return r.cfg.Active(ctx)
}

func (r *Runner) standby() {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.cfg.Now()
	if r.standbySince.IsZero() {
		r.standbySince = now
		return
	}
	if now.Sub(r.standbySince) >= standbyGrace {
		r.states = map[string]*state{}
	}
}

func (r *Runner) run(ctx context.Context, c Check) result {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.Timeout)
	defer cancel()
	start := time.Now()
	facts, err := c.Run(ctx)
	return result{facts: facts, err: err, ms: time.Since(start).Milliseconds()}
}

func (r *Runner) apply(ctx context.Context, name string, res result) {
	now := r.cfg.Now()
	r.mu.Lock()
	st, ok := r.states[name]
	if !ok {
		st = &state{alert: AlertNone, stale: true}
		r.states[name] = st
	}
	st.ms = res.ms
	if res.err == nil {
		st.failingSince, st.lastOK, st.lastError, st.facts = time.Time{}, now, "", res.facts
	} else {
		if st.failingSince.IsZero() {
			st.failingSince = now
		}
		st.lastError, st.facts = clip(res.err.Error()), nil
		st.incident = Incident{Check: name, Since: st.failingSince, Error: st.lastError}
	}
	r.mu.Unlock()

	switch {
	case res.err == nil:
		r.recovered(ctx, name, st)
	case now.Sub(st.incident.Since) >= r.cfg.FailAfter:
		r.escalate(ctx, st)
	default:
		r.cfg.Log.Warn("probe failing", "check", name, "since", st.incident.Since, "err", st.incident.Error)
	}
}

// escalate gets a failing incident to someone. The database comes first: it
// gives the alert its id, its history and the delivery retries. When it
// refuses, the direct announcement goes out once and storing keeps being tried
// on every tick.
func (r *Runner) escalate(ctx context.Context, st *state) {
	if st.alert == AlertStored {
		return
	}
	inc := st.incident
	inc.Quiet = st.alert == AlertDirect
	err := r.call(ctx, func(ctx context.Context) error { return r.cfg.Alerts.RaiseProbe(ctx, inc) })
	if err == nil {
		st.attempts = 0
		r.setAlert(st, AlertStored)
		r.cfg.Log.Error("probe alert raised", "check", inc.Check, "since", inc.Since, "err", inc.Error)
		return
	}
	st.attempts++
	r.cfg.Log.Error("probe alert not stored", "check", inc.Check, "attempt", st.attempts, "err", err)
	if st.alert == AlertDirect {
		return
	}
	r.setAlert(st, AlertUndelivered)
	if st.attempts < directAfter {
		return
	}
	if err := r.call(ctx, func(ctx context.Context) error { return r.cfg.Alerts.NotifyProbe(ctx, inc) }); err != nil {
		r.cfg.Log.Error("probe alert not delivered", "check", inc.Check, "err", err)
		return
	}
	r.setAlert(st, AlertDirect)
	r.cfg.Log.Warn("probe alert announced directly", "check", inc.Check)
}

// recovered closes what the incident left open. An incident that never reached
// the database still gets its row, quietly: announcing now something already
// over would only confuse, but the history must have it.
func (r *Runner) recovered(ctx context.Context, name string, st *state) {
	if st.alert == AlertDirect || st.alert == AlertUndelivered {
		inc := st.incident
		inc.Quiet = true
		if err := r.call(ctx, func(ctx context.Context) error { return r.cfg.Alerts.RaiseProbe(ctx, inc) }); err != nil {
			r.cfg.Log.Warn("probe history not stored", "check", name, "err", err)
			return
		}
		r.setAlert(st, AlertStored)
	}
	if st.alert == AlertStored || st.stale {
		if err := r.call(ctx, func(ctx context.Context) error { return r.cfg.Alerts.ClearProbe(ctx, name) }); err != nil {
			r.cfg.Log.Warn("probe alert not resolved", "check", name, "err", err)
			return
		}
		st.stale = false
	}
	st.attempts = 0
	r.setAlert(st, AlertNone)
}

func (r *Runner) call(ctx context.Context, fn func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.Timeout)
	defer cancel()
	return fn(ctx)
}

func (r *Runner) setAlert(st *state, alert string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st.alert = alert
}

// Snapshot returns the checks that have run, in configuration order. It is
// safe to call from any goroutine.
func (r *Runner) Snapshot() []Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Status, 0, len(r.cfg.Checks))
	for _, c := range r.cfg.Checks {
		st, ok := r.states[c.Name()]
		if !ok {
			continue
		}
		s := Status{
			Name: c.Name(), OK: st.failingSince.IsZero(), LastError: st.lastError, MS: st.ms,
			Alert: st.alert, Facts: st.facts,
		}
		if !st.failingSince.IsZero() {
			at := st.failingSince
			s.FailingSince = &at
		}
		if !st.lastOK.IsZero() {
			at := st.lastOK
			s.LastOKAt = &at
		}
		out = append(out, s)
	}
	return out
}

func clip(s string) string {
	const limit = 300
	r := []rune(s)
	if len(r) <= limit {
		return s
	}
	return string(r[:limit])
}
