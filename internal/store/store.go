// Package store owns itemwatch's own schema: alerts, the ledger checkpoint
// and the leader lock. It never touches the game tables.
package store

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

//go:embed migrations/*.sql
var migrations embed.FS

// ErrNotFound is returned when an alert does not exist.
var ErrNotFound = errors.New("not found")

// ErrConflict is returned when an alert is not in the expected status.
var ErrConflict = errors.New("conflict")

// Store wraps the state database.
type Store struct {
	db *sql.DB
}

// New returns a Store. Call Migrate before using it.
func New(db *sql.DB) *Store { return &Store{db: db} }

// Ping checks the connection.
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// Migrate applies the embedded migrations that have not run yet.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version VARCHAR(64) NOT NULL PRIMARY KEY,
		applied_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	for _, name := range names {
		var applied int
		if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM schema_migrations WHERE version = ?", name).Scan(&applied); err != nil {
			return err
		}
		if applied > 0 {
			continue
		}
		body, err := migrations.ReadFile("migrations/" + name)
		if err != nil {
			return err
		}
		// MariaDB commits DDL implicitly, so each statement stands alone; every
		// statement is written to be safe to repeat if a migration half-ran.
		for _, stmt := range strings.Split(string(body), ";\n") {
			if stmt = strings.TrimSpace(stmt); stmt == "" {
				continue
			}
			if _, err := s.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("migration %s: %w", name, err)
			}
		}
		if _, err := s.db.ExecContext(ctx, "INSERT INTO schema_migrations (version) VALUES (?)", name); err != nil {
			return err
		}
	}
	return nil
}

// Checkpoint is how far the ledger was processed and what the rules had
// learned at that point. Both are written in the same transaction as the
// alerts they produced.
type Checkpoint struct {
	Cursor    uint64
	RuleState map[string]json.RawMessage
}

// LoadCheckpoint returns the named checkpoint, or ok=false if none exists.
func (s *Store) LoadCheckpoint(ctx context.Context, name string) (cp Checkpoint, ok bool, err error) {
	var raw string
	err = s.db.QueryRowContext(ctx, "SELECT cursor_id, rule_state FROM checkpoints WHERE name = ?", name).Scan(&cp.Cursor, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return cp, false, nil
	}
	if err != nil {
		return cp, false, fmt.Errorf("load checkpoint: %w", err)
	}
	if err := json.Unmarshal([]byte(raw), &cp.RuleState); err != nil {
		return cp, false, fmt.Errorf("decode rule state: %w", err)
	}
	return cp, true, nil
}

// NewAlert is a rendered finding ready to be stored.
type NewAlert struct {
	DedupKey   string
	Rule       string
	Severity   int8
	Title      string
	Details    json.RawMessage
	PlayerID   int64
	PlayerName string
	ItemHash   string
	Origin     string
	Count      int64
	At         time.Time
}

// upsertAlert folds a finding into its incident. A resolved incident that
// happens again is reopened, gets a new generation and is published again.
// MariaDB evaluates these assignments left to right, so status goes last:
// the expressions before it still see the old status.
const upsertAlert = `INSERT INTO alerts
	(dedup_key, rule, severity, title, details, player_id, player_name, item_hash, origin, occurrences, first_seen, last_seen)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
	occurrences  = occurrences + VALUES(occurrences),
	last_seen    = GREATEST(last_seen, VALUES(last_seen)),
	severity     = GREATEST(severity, VALUES(severity)),
	generation   = generation + IF(status = 'resolved', 1, 0),
	published_at = IF(status = 'resolved', NULL, published_at),
	status       = 'open'`

// Commit stores alerts and, when cp is not nil, the named checkpoint, all in
// one transaction.
func (s *Store) Commit(ctx context.Context, name string, cp *Checkpoint, alerts []NewAlert) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if len(alerts) > 0 {
		stmt, err := tx.PrepareContext(ctx, upsertAlert)
		if err != nil {
			return fmt.Errorf("prepare upsert: %w", err)
		}
		defer stmt.Close()
		for _, a := range alerts {
			details := a.Details
			if len(details) == 0 {
				details = json.RawMessage("{}")
			}
			if _, err := stmt.ExecContext(ctx, a.DedupKey, a.Rule, a.Severity, truncate(a.Title, 255), string(details),
				a.PlayerID, truncate(a.PlayerName, 64), a.ItemHash, truncate(a.Origin, 255), a.Count, a.At, a.At); err != nil {
				return fmt.Errorf("upsert alert: %w", err)
			}
		}
	}

	if cp != nil {
		state, err := json.Marshal(cp.RuleState)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO checkpoints (name, cursor_id, rule_state, updated_at)
			VALUES (?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE cursor_id = VALUES(cursor_id), rule_state = VALUES(rule_state), updated_at = VALUES(updated_at)`,
			name, cp.Cursor, string(state), time.Now().UTC()); err != nil {
			return fmt.Errorf("save checkpoint: %w", err)
		}
	}
	return tx.Commit()
}

// Alert is a stored incident.
type Alert struct {
	ID             int64           `json:"id"`
	Rule           string          `json:"rule"`
	Severity       int8            `json:"-"`
	SeverityName   string          `json:"severity"`
	Title          string          `json:"title"`
	Details        json.RawMessage `json:"details"`
	PlayerID       int64           `json:"player_id"`
	PlayerName     string          `json:"player_name"`
	ItemHash       string          `json:"item_hash"`
	Origin         string          `json:"origin"`
	Occurrences    int64           `json:"occurrences"`
	Status         string          `json:"status"`
	Generation     int             `json:"generation"`
	FirstSeen      time.Time       `json:"first_seen"`
	LastSeen       time.Time       `json:"last_seen"`
	ResolvedAt     *time.Time      `json:"resolved_at"`
	ResolvedBy     string          `json:"resolved_by"`
	ResolutionNote string          `json:"resolution_note"`
	PublishedAt    *time.Time      `json:"-"`
}

const alertColumns = `id, rule, severity, title, details, player_id, player_name, item_hash, origin, occurrences,
	status, generation, first_seen, last_seen, resolved_at, resolved_by, resolution_note, published_at`

type scanner interface{ Scan(dest ...any) error }

func scanAlert(row scanner) (Alert, error) {
	var a Alert
	var details string
	var resolved, published sql.NullTime
	err := row.Scan(&a.ID, &a.Rule, &a.Severity, &a.Title, &details, &a.PlayerID, &a.PlayerName, &a.ItemHash,
		&a.Origin, &a.Occurrences, &a.Status, &a.Generation, &a.FirstSeen, &a.LastSeen, &resolved,
		&a.ResolvedBy, &a.ResolutionNote, &published)
	if err != nil {
		return a, err
	}
	a.Details = json.RawMessage(details)
	if resolved.Valid {
		a.ResolvedAt = &resolved.Time
	}
	if published.Valid {
		a.PublishedAt = &published.Time
	}
	return a, nil
}

func (s *Store) queryAlerts(ctx context.Context, q string, args ...any) ([]Alert, error) {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	alerts := []Alert{}
	for rows.Next() {
		a, err := scanAlert(rows)
		if err != nil {
			return nil, err
		}
		alerts = append(alerts, a)
	}
	return alerts, rows.Err()
}

// Unpublished returns alerts that still have to go to the queue.
func (s *Store) Unpublished(ctx context.Context, limit int) ([]Alert, error) {
	return s.queryAlerts(ctx, "SELECT "+alertColumns+" FROM alerts WHERE published_at IS NULL ORDER BY id LIMIT ?", limit)
}

// MarkPublished records that a generation of an alert reached the queue. A
// reopen in the meantime bumped the generation, so it is not marked.
func (s *Store) MarkPublished(ctx context.Context, id int64, generation int) error {
	_, err := s.db.ExecContext(ctx, "UPDATE alerts SET published_at = ? WHERE id = ? AND generation = ?", time.Now().UTC(), id, generation)
	return err
}

// Filter narrows ListAlerts.
type Filter struct {
	Status      string // open, resolved or empty for both
	MinSeverity int8
	Rule        string
	BeforeID    int64 // pagination: only ids lower than this
	Limit       int
}

// ListAlerts returns alerts newest first.
func (s *Store) ListAlerts(ctx context.Context, f Filter) ([]Alert, error) {
	var where []string
	var args []any
	if f.Status != "" {
		where = append(where, "status = ?")
		args = append(args, f.Status)
	}
	if f.MinSeverity > 0 {
		where = append(where, "severity >= ?")
		args = append(args, f.MinSeverity)
	}
	if f.Rule != "" {
		where = append(where, "rule = ?")
		args = append(args, f.Rule)
	}
	if f.BeforeID > 0 {
		where = append(where, "id < ?")
		args = append(args, f.BeforeID)
	}
	q := "SELECT " + alertColumns + " FROM alerts"
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	if f.Limit <= 0 || f.Limit > 200 {
		f.Limit = 50
	}
	q += " ORDER BY id DESC LIMIT ?"
	args = append(args, f.Limit)
	return s.queryAlerts(ctx, q, args...)
}

// GetAlert returns one alert.
func (s *Store) GetAlert(ctx context.Context, id int64) (Alert, error) {
	a, err := scanAlert(s.db.QueryRowContext(ctx, "SELECT "+alertColumns+" FROM alerts WHERE id = ?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return a, ErrNotFound
	}
	return a, err
}

// Resolve closes an open alert.
func (s *Store) Resolve(ctx context.Context, id int64, by, note string) (Alert, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE alerts SET status = 'resolved', resolved_at = ?, resolved_by = ?, resolution_note = ?
		WHERE id = ? AND status = 'open'`, time.Now().UTC(), truncate(by, 64), truncate(note, 500), id)
	if err != nil {
		return Alert{}, err
	}
	return s.afterTransition(ctx, id, res)
}

// Reopen puts a resolved alert back in the open list, without notifying again.
func (s *Store) Reopen(ctx context.Context, id int64) (Alert, error) {
	res, err := s.db.ExecContext(ctx, "UPDATE alerts SET status = 'open' WHERE id = ? AND status = 'resolved'", id)
	if err != nil {
		return Alert{}, err
	}
	return s.afterTransition(ctx, id, res)
}

func (s *Store) afterTransition(ctx context.Context, id int64, res sql.Result) (Alert, error) {
	n, err := res.RowsAffected()
	if err != nil {
		return Alert{}, err
	}
	a, err := s.GetAlert(ctx, id)
	if err != nil {
		return a, err
	}
	if n == 0 {
		return a, ErrConflict
	}
	return a, nil
}

// Stats summarizes the open alerts and the processing position.
type Stats struct {
	Open       map[string]int64 `json:"open"`
	OpenTotal  int64            `json:"open_total"`
	Cursor     uint64           `json:"cursor"`
	CursorTime *time.Time       `json:"checkpoint_at"`
}

// Stats counts open alerts per severity.
func (s *Store) Stats(ctx context.Context, checkpoint string, severityName func(int8) string) (Stats, error) {
	st := Stats{Open: map[string]int64{}}
	rows, err := s.db.QueryContext(ctx, "SELECT severity, COUNT(*) FROM alerts WHERE status = 'open' GROUP BY severity")
	if err != nil {
		return st, err
	}
	defer rows.Close()
	for rows.Next() {
		var sev int8
		var n int64
		if err := rows.Scan(&sev, &n); err != nil {
			return st, err
		}
		st.Open[severityName(sev)] = n
		st.OpenTotal += n
	}
	if err := rows.Err(); err != nil {
		return st, err
	}
	var at time.Time
	err = s.db.QueryRowContext(ctx, "SELECT cursor_id, updated_at FROM checkpoints WHERE name = ?", checkpoint).Scan(&st.Cursor, &at)
	if err == nil {
		st.CursorTime = &at
	} else if !errors.Is(err, sql.ErrNoRows) {
		return st, err
	}
	return st, nil
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}
