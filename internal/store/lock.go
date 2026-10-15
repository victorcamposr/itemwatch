package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ErrLockLost means the session holding the leader lock went away.
var ErrLockLost = errors.New("leader lock lost")

// RunAsLeader runs fn while holding a MariaDB named lock, so only one replica
// reads the ledger and writes the checkpoint. Other replicas wait and take
// over when the leader's connection drops. onLeader reports transitions.
func (s *Store) RunAsLeader(ctx context.Context, name string, retry time.Duration, onLeader func(bool), fn func(context.Context) error) error {
	for {
		err := s.leadOnce(ctx, name, onLeader, fn)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil && !errors.Is(err, ErrLockLost) && !errors.Is(err, errNotAcquired) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retry):
		}
	}
}

var errNotAcquired = errors.New("lock held by another replica")

func (s *Store) leadOnce(ctx context.Context, name string, onLeader func(bool), fn func(context.Context) error) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrLockLost, err)
	}
	defer conn.Close()

	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", name).Scan(&got); err != nil {
		return fmt.Errorf("%w: %v", ErrLockLost, err)
	}
	if !got.Valid || got.Int64 != 1 {
		return errNotAcquired
	}
	onLeader(true)
	defer onLeader(false)

	leadCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-leadCtx.Done():
				return
			case <-t.C:
				var mine sql.NullInt64
				err := conn.QueryRowContext(leadCtx, "SELECT IS_USED_LOCK(?) = CONNECTION_ID()", name).Scan(&mine)
				if leadCtx.Err() != nil {
					return
				}
				if err != nil || !mine.Valid || mine.Int64 != 1 {
					cancel(ErrLockLost)
					return
				}
			}
		}
	}()

	err = fn(leadCtx)
	if cause := context.Cause(leadCtx); errors.Is(cause, ErrLockLost) {
		return ErrLockLost
	}
	// Release explicitly so a standby takes over at once on shutdown.
	releaseCtx, done := context.WithTimeout(context.Background(), 2*time.Second)
	defer done()
	_, _ = conn.ExecContext(releaseCtx, "SELECT RELEASE_LOCK(?)", name)
	return err
}
