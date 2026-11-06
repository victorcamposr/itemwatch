package dispatch

import (
	"context"
	"errors"

	"github.com/victorcamposr/itemwatch/internal/queue"
)

// ErrNoChannel means no configured channel takes this severity.
var ErrNoChannel = errors.New("no notification channel for this severity")

// Direct delivers a notification without the stream, for what cannot wait for
// the database or Redis. It succeeds if any channel took it, because the point
// is that someone hears about it; the caller retries otherwise.
func Direct(ctx context.Context, notifiers []Notifier, n queue.Notification) error {
	var errs []error
	delivered := false
	for _, notifier := range notifiers {
		if n.Severity < notifier.MinSeverity() {
			continue
		}
		if err := notifier.Notify(ctx, n); err != nil {
			errs = append(errs, err)
			continue
		}
		delivered = true
	}
	if delivered {
		return nil
	}
	if len(errs) == 0 {
		return ErrNoChannel
	}
	return errors.Join(errs...)
}
