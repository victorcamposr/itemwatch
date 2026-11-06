package watcher

import (
	"context"

	"github.com/victorcamposr/itemwatch/internal/detect"
	"github.com/victorcamposr/itemwatch/internal/dispatch"
	"github.com/victorcamposr/itemwatch/internal/probe"
	"github.com/victorcamposr/itemwatch/internal/queue"
	"github.com/victorcamposr/itemwatch/internal/render"
)

// RaiseProbe stores a failing health check through the same path as the rules:
// one row per check, folded by dedup key, published after the commit. It does
// not touch the game database, which may be the thing that is down.
func (w *Watcher) RaiseProbe(ctx context.Context, inc probe.Incident) error {
	f := detect.ProbeFinding(inc.Check, inc.Since, inc.Error)
	alerts, err := w.toAlerts(ctx, []detect.Finding{f})
	if err != nil {
		return err
	}
	alerts[0].Quiet = inc.Quiet
	if err := w.Store.Commit(ctx, CheckpointName, nil, alerts); err != nil {
		return err
	}
	w.Metrics.Findings.WithLabelValues(f.Rule, f.Severity.String()).Inc()
	w.PublishPending(ctx)
	return nil
}

// ClearProbe resolves the check's open alert, if there is one.
func (w *Watcher) ClearProbe(ctx context.Context, check string) error {
	_, err := w.Store.ResolveByKey(ctx, detect.ProbeKey(check), detect.ProbeRule, "check recovered")
	return err
}

// NotifyProbe is the last resort for an alert the state database refused: it
// goes straight to the channels, with no id, no retry and no idempotency key.
// Run the same outage on two replicas and each announces it once.
func (w *Watcher) NotifyProbe(ctx context.Context, inc probe.Incident) error {
	f := detect.ProbeFinding(inc.Check, inc.Since, inc.Error)
	return dispatch.Direct(ctx, w.Direct, queue.Notification{
		Rule: f.Rule, Severity: int8(f.Severity), Title: render.Title(w.Lang, f),
		Origin: f.Origin, Occurrences: f.Count, FirstSeen: f.At,
	})
}
