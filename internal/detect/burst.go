package detect

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/victorcamposr/itemwatch/internal/ledger"
)

// Limit caps how many items one player may receive from an origin within a
// window. "quest:=3/24h" means: more than 3 births from any quest:* origin to
// the same player in 24 hours is an incident.
type Limit struct {
	Pattern Pattern
	Max     int
	Window  time.Duration
}

// ParseLimits reads "pattern=max/window" entries separated by commas and
// orders them from the most to the least specific pattern.
func ParseLimits(spec string) ([]Limit, error) {
	var limits []Limit
	for _, entry := range strings.Split(spec, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		pattern, rest, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, fmt.Errorf("limit %q: want pattern=max/window", entry)
		}
		maxText, windowText, ok := strings.Cut(rest, "/")
		if !ok {
			return nil, fmt.Errorf("limit %q: want pattern=max/window", entry)
		}
		max, err := strconv.Atoi(strings.TrimSpace(maxText))
		if err != nil || max < 1 {
			return nil, fmt.Errorf("limit %q: max must be a positive integer", entry)
		}
		window, err := time.ParseDuration(strings.TrimSpace(windowText))
		if err != nil || window < time.Minute {
			return nil, fmt.Errorf("limit %q: window must be a duration of at least 1m", entry)
		}
		limits = append(limits, Limit{Pattern: Pattern(strings.TrimSpace(pattern)), Max: max, Window: window})
	}
	sort.SliceStable(limits, func(i, j int) bool {
		return limits[i].Pattern.specificity() > limits[j].Pattern.specificity()
	})
	return limits, nil
}

// bucketsPerWindow sets the precision of the sliding window. The window is
// approximated by fixed sub-buckets so memory stays bounded however many
// events arrive.
const bucketsPerWindow = 20

// Burst detects one player receiving too many items from one origin: a quest
// reward paid twice, a script looping, a farm abusing a bug.
type Burst struct {
	limits []Limit
	ignore Patterns
	state  burstState
}

type burstState struct {
	Watermark int64                   `json:"watermark"`
	Keys      map[string]*burstWindow `json:"keys"`
}

type burstWindow struct {
	Origin    string        `json:"origin"`
	PlayerID  int64         `json:"player_id"`
	Limit     int           `json:"limit"`
	Buckets   []burstBucket `json:"buckets"`
	LastAlert int64         `json:"last_alert"`
}

type burstBucket struct {
	Start    int64 `json:"start"`
	Events   int   `json:"events"`
	Quantity int64 `json:"quantity"`
}

// NewBurst builds the rule. Origins matching ignore are never counted.
func NewBurst(limits []Limit, ignore Patterns) *Burst {
	return &Burst{limits: limits, ignore: ignore, state: burstState{Keys: map[string]*burstWindow{}}}
}

func (*Burst) Name() string { return "burst" }

func (b *Burst) limitFor(origin string) int {
	for i, l := range b.limits {
		if l.Pattern.Match(origin) {
			return i
		}
	}
	return -1
}

func (b *Burst) Observe(ev ledger.Event) []Finding {
	if ev.Kind != "birth" || ev.PlayerID == 0 || b.ignore.Match(ev.Origin) {
		return nil
	}
	idx := b.limitFor(ev.Origin)
	if idx < 0 {
		return nil
	}
	limit := b.limits[idx]
	origin := originKey(ev)
	now := ev.CreatedAt.Unix()
	if now > b.state.Watermark {
		b.state.Watermark = now
	}

	key := origin + "\x00" + strconv.FormatInt(ev.PlayerID, 10)
	w := b.state.Keys[key]
	if w == nil {
		w = &burstWindow{Origin: origin, PlayerID: ev.PlayerID, Limit: idx}
		b.state.Keys[key] = w
	}

	windowSec := int64(limit.Window / time.Second)
	width := max(windowSec/bucketsPerWindow, 1)
	start := now - now%width
	w.evict(now - windowSec)
	if n := len(w.Buckets); n > 0 && w.Buckets[n-1].Start == start {
		w.Buckets[n-1].Events++
		w.Buckets[n-1].Quantity += int64(ev.Count)
	} else {
		w.Buckets = append(w.Buckets, burstBucket{Start: start, Events: 1, Quantity: int64(ev.Count)})
	}

	events, quantity := w.totals()
	if events <= limit.Max || (w.LastAlert != 0 && now-w.LastAlert < windowSec) {
		return nil
	}
	w.LastAlert = now
	return []Finding{{
		Rule:     "burst",
		Severity: Warning,
		Key:      dedupKey("burst", origin, strconv.FormatInt(ev.PlayerID, 10), strconv.FormatInt(now/windowSec, 10)),
		At:       ev.CreatedAt,
		Count:    1,
		PlayerID: ev.PlayerID,
		ItemHash: ev.Hash,
		Origin:   origin,
		Facts: map[string]any{
			"events":    events,
			"quantity":  quantity,
			"limit":     limit.Max,
			"window":    FormatDuration(limit.Window),
			"pattern":   string(limit.Pattern),
			"item_type": ev.ItemType,
		},
	}}
}

func (w *burstWindow) evict(before int64) {
	i := 0
	for i < len(w.Buckets) && w.Buckets[i].Start <= before {
		i++
	}
	w.Buckets = w.Buckets[i:]
}

func (w *burstWindow) totals() (events int, quantity int64) {
	for _, b := range w.Buckets {
		events += b.Events
		quantity += b.Quantity
	}
	return events, quantity
}

// SaveState drops windows that went quiet and serializes the rest.
func (b *Burst) SaveState() (json.RawMessage, error) {
	for key, w := range b.state.Keys {
		if w.Limit >= len(b.limits) {
			delete(b.state.Keys, key)
			continue
		}
		w.evict(b.state.Watermark - int64(b.limits[w.Limit].Window/time.Second))
		if len(w.Buckets) == 0 {
			delete(b.state.Keys, key)
		}
	}
	return json.Marshal(b.state)
}

// LoadState restores a snapshot. Windows whose limit index no longer exists
// (the configuration changed) are dropped by the next SaveState.
func (b *Burst) LoadState(raw json.RawMessage) error {
	st := burstState{Keys: map[string]*burstWindow{}}
	if err := json.Unmarshal(raw, &st); err != nil {
		return err
	}
	if st.Keys == nil {
		st.Keys = map[string]*burstWindow{}
	}
	b.state = st
	return nil
}

// FormatDuration prints 24h, 10m or 90s instead of Go's 24h0m0s.
func FormatDuration(d time.Duration) string {
	switch {
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", d/time.Hour)
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", d/time.Minute)
	}
	return d.String()
}
