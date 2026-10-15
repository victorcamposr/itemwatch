package detect

import (
	"strconv"
	"strings"
	"time"

	"github.com/victorcamposr/itemwatch/internal/ledger"
)

// Unhashed flags tracked items that reached a save or a move without a hash.
// It always means a code path skipped the item factory: a bug to fix, and a
// hole an exploit could go through untraced.
type Unhashed struct{}

func (Unhashed) Name() string { return "unhashed" }

func (Unhashed) Observe(ev ledger.Event) []Finding {
	if ev.Kind != "unhashed" {
		return nil
	}
	day := ev.CreatedAt.UTC().Format(time.DateOnly)
	return []Finding{{
		Rule:     "unhashed",
		Severity: Warning,
		Key:      dedupKey("unhashed", strconv.FormatUint(uint64(ev.ItemType), 10), ev.Origin, ev.Source, day),
		At:       ev.CreatedAt,
		Count:    1,
		PlayerID: ev.PlayerID,
		Origin:   originKey(ev),
		Facts:    map[string]any{"item_type": ev.ItemType, "source": ev.Source, "day": day},
	}}
}

// StaffCreation records every item a staff command created, grouped per
// command and hour. Informational: staff creating items is allowed, but the
// team wants it on record.
type StaffCreation struct {
	Prefix string // origin prefix of staff commands, "gm:" by default
}

func (StaffCreation) Name() string { return "staff" }

func (r StaffCreation) Observe(ev ledger.Event) []Finding {
	prefix := r.Prefix
	if prefix == "" {
		prefix = "gm:"
	}
	if ev.Kind != "birth" || !strings.HasPrefix(ev.Origin, prefix) {
		return nil
	}
	hour := ev.CreatedAt.Unix() / 3600
	return []Finding{{
		Rule:     "staff",
		Severity: Info,
		Key:      dedupKey("staff", ev.Origin, strconv.FormatInt(ev.PlayerID, 10), strconv.FormatInt(hour, 10)),
		At:       ev.CreatedAt,
		Count:    1,
		PlayerID: ev.PlayerID,
		ItemHash: ev.Hash,
		Origin:   ev.Origin,
		Facts:    map[string]any{"item_type": ev.ItemType, "count": ev.Count},
	}}
}
