// Package dispatch consumes the alert stream and delivers each notification
// to the configured channels.
//
// Delivery guarantees:
//   - at-least-once from the stream: a message is acknowledged only after
//     every channel succeeded or failed for good; until then it stays in the
//     consumer group's pending list and is claimed again with backoff;
//   - effectively-once per channel: an idempotency key in Redis
//     (alert id + generation + channel) records successful deliveries, so a
//     redelivered or republished message is not announced twice;
//   - poison messages go to a dead-letter stream after MaxAttempts.
package dispatch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"

	"github.com/victorcamposr/itemwatch/internal/queue"
)

// Notifier delivers a notification to one channel.
type Notifier interface {
	Name() string
	MinSeverity() int8
	Notify(ctx context.Context, n queue.Notification) error
}

// PermanentError marks a failure that retrying cannot fix (bad request,
// revoked webhook).
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return "permanent: " + e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err as a PermanentError.
func Permanent(err error) error { return &PermanentError{Err: err} }

// Recorder receives delivery outcomes, for metrics.
type Recorder interface {
	Delivery(channel, result string)
}

// Config tunes the dispatcher.
type Config struct {
	Stream      string
	DeadLetter  string
	Group       string
	Consumer    string
	KeyPrefix   string
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	MaxAttempts int64
	InflightTTL time.Duration
	DoneTTL     time.Duration
	Block       time.Duration
	RetryEvery  time.Duration
}

func (c *Config) defaults() {
	if c.BaseBackoff <= 0 {
		c.BaseBackoff = 15 * time.Second
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = 15 * time.Minute
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 8
	}
	if c.InflightTTL <= 0 {
		c.InflightTTL = 2 * time.Minute
	}
	if c.DoneTTL <= 0 {
		c.DoneTTL = 30 * 24 * time.Hour
	}
	if c.Block <= 0 {
		c.Block = 5 * time.Second
	}
	if c.RetryEvery <= 0 {
		c.RetryEvery = 5 * time.Second
	}
	if c.KeyPrefix == "" {
		c.KeyPrefix = "itemwatch"
	}
}

// Dispatcher reads the stream as one consumer of a consumer group.
type Dispatcher struct {
	rdb       *redis.Client
	cfg       Config
	notifiers []Notifier
	log       *slog.Logger
	rec       Recorder
}

// New builds a Dispatcher.
func New(rdb *redis.Client, cfg Config, notifiers []Notifier, log *slog.Logger, rec Recorder) *Dispatcher {
	cfg.defaults()
	return &Dispatcher{rdb: rdb, cfg: cfg, notifiers: notifiers, log: log, rec: rec}
}

// Backoff returns the idle time a message must reach before attempt n+1.
func (d *Dispatcher) Backoff(deliveries int64) time.Duration {
	b := d.cfg.BaseBackoff
	for i := int64(1); i < deliveries && b < d.cfg.MaxBackoff; i++ {
		b *= 2
	}
	return min(b, d.cfg.MaxBackoff)
}

// Run consumes until ctx ends.
func (d *Dispatcher) Run(ctx context.Context) error {
	err := d.rdb.XGroupCreateMkStream(ctx, d.cfg.Stream, d.cfg.Group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create consumer group: %w", err)
	}
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return d.readNew(ctx) })
	g.Go(func() error { return d.retryPending(ctx) })
	return g.Wait()
}

func (d *Dispatcher) readNew(ctx context.Context) error {
	for ctx.Err() == nil {
		streams, err := d.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: d.cfg.Group, Consumer: d.cfg.Consumer,
			Streams: []string{d.cfg.Stream, ">"}, Count: 20, Block: d.cfg.Block,
		}).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			d.log.Warn("read stream", "err", err)
			sleep(ctx, time.Second)
			continue
		}
		for _, s := range streams {
			for _, msg := range s.Messages {
				d.handle(ctx, msg, 1)
			}
		}
	}
	return nil
}

// retryPending claims messages whose last delivery failed (or whose consumer
// died) once they have been idle for their backoff.
func (d *Dispatcher) retryPending(ctx context.Context) error {
	t := time.NewTicker(d.cfg.RetryEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		if err := d.RetryOnce(ctx); err != nil && ctx.Err() == nil {
			d.log.Warn("retry pending", "err", err)
		}
	}
}

// RetryOnce runs one pass over the pending list.
func (d *Dispatcher) RetryOnce(ctx context.Context) error {
	pending, err := d.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: d.cfg.Stream, Group: d.cfg.Group, Start: "-", End: "+", Count: 100,
	}).Result()
	if err != nil {
		return err
	}
	for _, p := range pending {
		if p.RetryCount >= d.cfg.MaxAttempts {
			d.deadLetterByID(ctx, p.ID, fmt.Sprintf("gave up after %d deliveries", p.RetryCount))
			continue
		}
		wait := d.Backoff(p.RetryCount)
		if p.Idle < wait {
			continue
		}
		claimed, err := d.rdb.XClaim(ctx, &redis.XClaimArgs{
			Stream: d.cfg.Stream, Group: d.cfg.Group, Consumer: d.cfg.Consumer,
			MinIdle: wait, Messages: []string{p.ID},
		}).Result()
		if err != nil {
			return err
		}
		if len(claimed) == 0 {
			// Trimmed from the stream while pending, or claimed by another
			// consumer first. Acknowledge only the trimmed case.
			if exists, _ := d.exists(ctx, p.ID); !exists {
				d.rdb.XAck(ctx, d.cfg.Stream, d.cfg.Group, p.ID)
			}
			continue
		}
		for _, msg := range claimed {
			d.handle(ctx, msg, p.RetryCount+1)
		}
	}
	return nil
}

func (d *Dispatcher) exists(ctx context.Context, id string) (bool, error) {
	msgs, err := d.rdb.XRangeN(ctx, d.cfg.Stream, id, id, 1).Result()
	return len(msgs) > 0, err
}

type outcome int

const (
	delivered outcome = iota
	retryLater
	failedForGood
)

// handle tries every channel and acknowledges when nothing is left to retry.
func (d *Dispatcher) handle(ctx context.Context, msg redis.XMessage, attempt int64) {
	n, err := queue.Decode(msg)
	if err != nil {
		d.deadLetter(ctx, msg, err.Error())
		return
	}
	retry, failed := false, false
	for _, notifier := range d.notifiers {
		switch d.deliver(ctx, notifier, n, attempt) {
		case retryLater:
			retry = true
		case failedForGood:
			failed = true
		}
	}
	switch {
	case retry:
		// Stay pending; retryPending claims it again after the backoff.
	case failed:
		d.deadLetter(ctx, msg, "a channel rejected the notification")
	default:
		d.rdb.XAck(ctx, d.cfg.Stream, d.cfg.Group, msg.ID)
	}
}

func (d *Dispatcher) deliver(ctx context.Context, notifier Notifier, n queue.Notification, attempt int64) outcome {
	channel := notifier.Name()
	if n.Severity < notifier.MinSeverity() {
		d.record(channel, "skipped")
		return delivered
	}
	key := n.IdempotencyKey(d.cfg.KeyPrefix, channel)
	won, err := d.rdb.SetNX(ctx, key, "inflight", d.cfg.InflightTTL).Result()
	if err != nil {
		d.log.Warn("idempotency key", "err", err, "alert", n.AlertID)
		return retryLater
	}
	if !won {
		state, _ := d.rdb.Get(ctx, key).Result()
		if state == "done" {
			d.record(channel, "duplicate")
			return delivered
		}
		// Another consumer is delivering it right now.
		return retryLater
	}

	err = notifier.Notify(ctx, n)
	if err == nil {
		d.rdb.Set(ctx, key, "done", d.cfg.DoneTTL)
		d.record(channel, "delivered")
		d.log.Info("alert delivered", "channel", channel, "alert", n.AlertID, "generation", n.Generation, "attempt", attempt)
		return delivered
	}
	d.rdb.Del(ctx, key)
	var permanent *PermanentError
	if errors.As(err, &permanent) {
		d.record(channel, "rejected")
		d.log.Error("alert rejected", "channel", channel, "alert", n.AlertID, "err", err)
		return failedForGood
	}
	d.record(channel, "failed")
	d.log.Warn("alert delivery failed", "channel", channel, "alert", n.AlertID, "attempt", attempt,
		"retry_in", d.Backoff(attempt), "err", err)
	return retryLater
}

func (d *Dispatcher) deadLetterByID(ctx context.Context, id, reason string) {
	msgs, err := d.rdb.XRangeN(ctx, d.cfg.Stream, id, id, 1).Result()
	if err != nil {
		d.log.Warn("read dead letter", "id", id, "err", err)
		return
	}
	if len(msgs) == 0 {
		d.rdb.XAck(ctx, d.cfg.Stream, d.cfg.Group, id)
		return
	}
	d.deadLetter(ctx, msgs[0], reason)
}

func (d *Dispatcher) deadLetter(ctx context.Context, msg redis.XMessage, reason string) {
	values := map[string]any{"original_id": msg.ID, "reason": reason, "failed_at": time.Now().UTC().Format(time.RFC3339)}
	for k, v := range msg.Values {
		values[k] = v
	}
	if err := d.rdb.XAdd(ctx, &redis.XAddArgs{Stream: d.cfg.DeadLetter, MaxLen: 10000, Approx: true, Values: values}).Err(); err != nil {
		d.log.Error("dead letter", "id", msg.ID, "err", err)
		return
	}
	d.rdb.XAck(ctx, d.cfg.Stream, d.cfg.Group, msg.ID)
	d.record("stream", "dead_letter")
	d.log.Error("notification dead-lettered", "id", msg.ID, "reason", reason)
}

func (d *Dispatcher) record(channel, result string) {
	if d.rec != nil {
		d.rec.Delivery(channel, result)
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
