package detect

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/victorcamposr/itemwatch/internal/ledger"
)

// LedgerResetRule names the alert the watcher raises when the ledger ids go
// back below the cursor (the table was rebuilt or emptied).
const LedgerResetRule = "ledger"

// LedgerResetKey is one incident per cursor the reset was found at.
func LedgerResetKey(cursor uint64) string {
	return dedupKey(LedgerResetRule, strconv.FormatUint(cursor, 10))
}

// Once flags origins that should hand items to a character a single time,
// such as a starter kit. The first delivery opens a window (a kit is many
// items written over a few seconds); a delivery from the same origin to the
// same player after the window is an incident. Volume rules make no sense for
// these origins: many new characters at once look like a spike, and none of
// them is wrong.
type Once struct {
	Patterns Patterns
	Window   time.Duration
	first    map[string]int64 // origin + player -> unix second of the first item
}

// DefaultOnceWindow covers one delivery of a kit.
const DefaultOnceWindow = 10 * time.Minute

// NewOnce builds the rule; a zero window takes the default.
func NewOnce(patterns Patterns, window time.Duration) *Once {
	if window <= 0 {
		window = DefaultOnceWindow
	}
	return &Once{Patterns: patterns, Window: window, first: map[string]int64{}}
}

func (*Once) Name() string { return "repeat" }

func (r *Once) Observe(ev ledger.Event) []Finding {
	if ev.Kind != "birth" || ev.PlayerID <= 0 || !r.Patterns.Match(ev.Origin) {
		return nil
	}
	key := ev.Origin + "\x00" + strconv.FormatInt(ev.PlayerID, 10)
	now := ev.CreatedAt.Unix()
	first, seen := r.first[key]
	if !seen {
		r.first[key] = now
		return nil
	}
	if now-first <= int64(r.Window/time.Second) {
		return nil
	}
	return []Finding{{
		Rule:     "repeat",
		Severity: Warning,
		Key:      dedupKey("repeat", ev.Origin, strconv.FormatInt(ev.PlayerID, 10)),
		At:       ev.CreatedAt,
		Count:    1,
		PlayerID: ev.PlayerID,
		ItemHash: ev.Hash,
		Origin:   ev.Origin,
		Facts:    map[string]any{"first_at": time.Unix(first, 0).UTC().Format(time.RFC3339), "item_type": ev.ItemType},
	}}
}

func (r *Once) SaveState() (json.RawMessage, error) { return json.Marshal(r.first) }

func (r *Once) LoadState(raw json.RawMessage) error {
	first := map[string]int64{}
	if err := json.Unmarshal(raw, &first); err != nil {
		r.first = map[string]int64{}
		return err
	}
	r.first = first
	return nil
}
