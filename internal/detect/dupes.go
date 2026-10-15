package detect

import (
	"time"

	"github.com/victorcamposr/itemwatch/internal/ledger"
)

// DupeTracker turns periodic duplicate scans into confirmed incidents.
//
// The item tables reflect each character's last save, and characters save at
// different times. Right after a trade the item can show up under both the
// old owner (not saved yet) and the new one. A real duplicate survives every
// save, so a hash is only reported after it stayed duplicated in every scan
// for ConfirmAfter, which should exceed the server's save interval.
type DupeTracker struct {
	ConfirmAfter time.Duration
	firstSeen    map[string]time.Time
	reported     map[string]bool
}

// NewDupeTracker returns a tracker with the given confirmation delay.
func NewDupeTracker(confirmAfter time.Duration) *DupeTracker {
	return &DupeTracker{
		ConfirmAfter: confirmAfter,
		firstSeen:    map[string]time.Time{},
		reported:     map[string]bool{},
	}
}

// Scan takes the duplicates found at time now and returns newly confirmed
// ones. A hash that disappears from a scan starts over.
func (d *DupeTracker) Scan(now time.Time, dups []ledger.Duplicate) []Finding {
	present := make(map[string]bool, len(dups))
	var findings []Finding
	for _, dup := range dups {
		present[dup.Hash] = true
		first, seen := d.firstSeen[dup.Hash]
		if !seen {
			d.firstSeen[dup.Hash] = now
			first = now
		}
		if d.reported[dup.Hash] || now.Sub(first) < d.ConfirmAfter {
			continue
		}
		d.reported[dup.Hash] = true
		var player int64
		if len(dup.Holders) > 0 {
			player = dup.Holders[0].PlayerID
		}
		findings = append(findings, Finding{
			Rule:     "dupe",
			Severity: Critical,
			Key:      dedupKey("dupe", dup.Hash),
			At:       now,
			Count:    1,
			PlayerID: player,
			ItemHash: dup.Hash,
			Facts: map[string]any{
				"holders":    dup.Holders,
				"places":     len(dup.Holders),
				"first_seen": first.UTC().Format(time.RFC3339),
			},
		})
	}
	for hash := range d.firstSeen {
		if !present[hash] {
			delete(d.firstSeen, hash)
			delete(d.reported, hash)
		}
	}
	return findings
}

// Tracked returns how many hashes are currently duplicated, confirmed or not.
func (d *DupeTracker) Tracked() int { return len(d.firstSeen) }
