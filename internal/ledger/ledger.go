// Package ledger reads the game server's item event ledger and the tables that
// hold item instances. It only ever issues SELECTs: the game database is
// owned by the game server.
package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Event is one row of the ledger. Every pickupable item carries a 16-hex
// tracking hash stamped at creation; the server appends one row per lifecycle
// event (birth, split, clone, market-in, ...).
type Event struct {
	ID        uint64
	Hash      string
	Kind      string // the `event` column: birth, split, unhashed, ...
	Parent    string
	ItemType  uint32
	Count     uint32
	Origin    string // what created the item: loot:<monster>, quest:<name>, gm:<name> <cmd>, script, ...
	Source    string // script file when Origin is "script"
	PlayerID  int64
	CreatedAt time.Time
}

// Holder is one place where an item instance currently lives.
type Holder struct {
	Place    string `json:"place"`
	PlayerID int64  `json:"player_id"`
}

// Duplicate is a tracking hash held in more than one place at once.
type Duplicate struct {
	Hash    string
	Holders []Holder
}

// HoldingSource is a query returning (hash, player_id) for every item stored
// in one place.
type HoldingSource struct {
	Place string
	Query string
}

// DefaultHoldingSources matches the Crystal Server schema with the hash column
// added to every item table.
var DefaultHoldingSources = []HoldingSource{
	{"inventory", "SELECT `hash`, `player_id` FROM `player_items` WHERE `hash` <> ''"},
	{"depot", "SELECT `hash`, `player_id` FROM `player_depotitems` WHERE `hash` <> ''"},
	{"inbox", "SELECT `hash`, `player_id` FROM `player_inboxitems` WHERE `hash` <> ''"},
	{"reward_chest", "SELECT `hash`, `player_id` FROM `player_rewards` WHERE `hash` <> ''"},
	{"market", "SELECT `i`.`hash`, `o`.`player_id` FROM `market_offer_items` `i` JOIN `market_offers` `o` ON `o`.`id` = `i`.`offer_id` WHERE `i`.`hash` <> ''"},
}

var identifier = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// Reader runs the read-only queries against the game database.
type Reader struct {
	db      *sql.DB
	table   string
	sources []HoldingSource
}

// NewReader returns a Reader for the given ledger table.
func NewReader(db *sql.DB, table string, sources []HoldingSource) (*Reader, error) {
	if !identifier.MatchString(table) {
		return nil, fmt.Errorf("invalid ledger table name %q", table)
	}
	if len(sources) == 0 {
		sources = DefaultHoldingSources
	}
	return &Reader{db: db, table: table, sources: sources}, nil
}

// After returns up to limit events with id greater than after, in id order.
func (r *Reader) After(ctx context.Context, after uint64, limit int) ([]Event, error) {
	q := "SELECT `id`, `hash`, `event`, `parent`, `itemtype`, `count`, `origin`, `source`, `player_id`, `created_at` FROM `" +
		r.table + "` WHERE `id` > ? ORDER BY `id` LIMIT ?"
	rows, err := r.db.QueryContext(ctx, q, after, limit)
	if err != nil {
		return nil, fmt.Errorf("read ledger: %w", err)
	}
	defer rows.Close()

	events := make([]Event, 0, limit)
	for rows.Next() {
		var ev Event
		var created int64
		if err := rows.Scan(&ev.ID, &ev.Hash, &ev.Kind, &ev.Parent, &ev.ItemType, &ev.Count,
			&ev.Origin, &ev.Source, &ev.PlayerID, &created); err != nil {
			return nil, fmt.Errorf("scan ledger row: %w", err)
		}
		ev.CreatedAt = time.Unix(created, 0).UTC()
		events = append(events, ev)
	}
	return events, rows.Err()
}

// MaxID returns the newest ledger id, or 0 for an empty ledger.
func (r *Reader) MaxID(ctx context.Context) (uint64, error) {
	var id uint64
	err := r.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(`id`), 0) FROM `"+r.table+"`").Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("ledger max id: %w", err)
	}
	return id, nil
}

// Duplicates lists hashes found in more than one place, as of the last time
// the game server saved each holder.
func (r *Reader) Duplicates(ctx context.Context, limit int) ([]Duplicate, error) {
	parts := make([]string, len(r.sources))
	for i, s := range r.sources {
		parts[i] = fmt.Sprintf("SELECT '%s' AS `place`, `s%d`.* FROM (%s) AS `s%d`", s.Place, i, s.Query, i)
	}
	q := "SELECT `hash`, GROUP_CONCAT(CONCAT(`place`, ':', `player_id`) SEPARATOR ',') FROM (" +
		strings.Join(parts, " UNION ALL ") + ") AS `t` GROUP BY `hash` HAVING COUNT(*) > 1 LIMIT ?"
	rows, err := r.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("scan duplicates: %w", err)
	}
	defer rows.Close()

	var dups []Duplicate
	for rows.Next() {
		var hash, list string
		if err := rows.Scan(&hash, &list); err != nil {
			return nil, fmt.Errorf("scan duplicate row: %w", err)
		}
		dups = append(dups, Duplicate{Hash: hash, Holders: parseHolders(list)})
	}
	return dups, rows.Err()
}

func parseHolders(list string) []Holder {
	var holders []Holder
	for _, item := range strings.Split(list, ",") {
		place, id, ok := strings.Cut(item, ":")
		if !ok {
			continue
		}
		pid, _ := strconv.ParseInt(id, 10, 64)
		holders = append(holders, Holder{Place: place, PlayerID: pid})
	}
	return holders
}

// PlayerNames resolves character ids to names. Unknown ids are left out.
func (r *Reader) PlayerNames(ctx context.Context, ids []int64) (map[int64]string, error) {
	names := make(map[int64]string, len(ids))
	if len(ids) == 0 {
		return names, nil
	}
	args := make([]any, len(ids))
	marks := make([]string, len(ids))
	for i, id := range ids {
		args[i] = id
		marks[i] = "?"
	}
	rows, err := r.db.QueryContext(ctx, "SELECT `id`, `name` FROM `players` WHERE `id` IN ("+strings.Join(marks, ",")+")", args...)
	if err != nil {
		return nil, fmt.Errorf("player names: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		names[id] = name
	}
	return names, rows.Err()
}

// Ping checks the connection.
func (r *Reader) Ping(ctx context.Context) error { return r.db.PingContext(ctx) }
