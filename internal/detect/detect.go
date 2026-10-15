// Package detect holds the anomaly rules. Stream rules see every ledger event
// once, in id order, and keep their own state; that state is checkpointed
// together with the ledger cursor so a restart neither skips nor repeats work.
package detect

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/victorcamposr/itemwatch/internal/ledger"
)

// Severity orders findings. Notification channels have a minimum severity.
type Severity int8

const (
	Info Severity = iota + 1
	Warning
	Critical
)

func (s Severity) String() string {
	switch s {
	case Info:
		return "info"
	case Warning:
		return "warning"
	case Critical:
		return "critical"
	}
	return fmt.Sprintf("severity(%d)", int8(s))
}

// ParseSeverity accepts the names returned by String.
func ParseSeverity(s string) (Severity, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "info":
		return Info, nil
	case "warning", "warn":
		return Warning, nil
	case "critical":
		return Critical, nil
	}
	return 0, fmt.Errorf("unknown severity %q", s)
}

// Finding is one anomaly occurrence. Findings that share a Key are the same
// incident: the store folds them into one alert and counts occurrences.
type Finding struct {
	Rule     string
	Severity Severity
	Key      string
	At       time.Time
	Count    int64
	PlayerID int64
	ItemHash string
	Origin   string
	Facts    map[string]any
}

// Rule consumes ledger events in order.
type Rule interface {
	Name() string
	Observe(ev ledger.Event) []Finding
}

// Stateful rules can save and restore what they learned.
type Stateful interface {
	SaveState() (json.RawMessage, error)
	LoadState(json.RawMessage) error
}

// dedupKey builds a fixed-size incident key from arbitrary parts.
func dedupKey(rule string, parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return rule + ":" + hex.EncodeToString(sum[:16])
}

// originKey is the label a rule groups by. Unlabelled script creations are
// told apart by the script file that ran.
func originKey(ev ledger.Event) string {
	if (ev.Origin == "script" || ev.Origin == "server") && ev.Source != "" {
		return ev.Origin + ":" + ev.Source
	}
	return ev.Origin
}

// Pattern matches an origin label: "*" matches anything, a pattern ending in
// ':' or '*' is a prefix, anything else must match exactly.
type Pattern string

func (p Pattern) Match(origin string) bool {
	s := string(p)
	switch {
	case s == "*":
		return true
	case strings.HasSuffix(s, "*"):
		return strings.HasPrefix(origin, strings.TrimSuffix(s, "*"))
	case strings.HasSuffix(s, ":"):
		return strings.HasPrefix(origin, s)
	}
	return origin == s
}

// specificity ranks patterns so the most specific one wins.
func (p Pattern) specificity() int {
	s := string(p)
	switch {
	case s == "*":
		return 0
	case strings.HasSuffix(s, "*"), strings.HasSuffix(s, ":"):
		return 1 + len(s)
	}
	return 1 << 20
}

// Patterns is a set of patterns; it matches when any member does.
type Patterns []Pattern

// ParsePatterns splits a comma separated list.
func ParsePatterns(csv string) Patterns {
	var ps Patterns
	for _, part := range strings.Split(csv, ",") {
		if part = strings.TrimSpace(part); part != "" {
			ps = append(ps, Pattern(part))
		}
	}
	return ps
}

func (ps Patterns) Match(origin string) bool {
	for _, p := range ps {
		if p.Match(origin) {
			return true
		}
	}
	return false
}
