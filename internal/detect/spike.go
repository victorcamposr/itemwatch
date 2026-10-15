package detect

import (
	"encoding/json"
	"math"
	"strconv"
	"time"

	"github.com/victorcamposr/itemwatch/internal/ledger"
)

// SpikeConfig tunes the per-origin rate detector.
type SpikeConfig struct {
	Bucket    time.Duration // rate resolution, 1m by default
	Alpha     float64       // EWMA smoothing: higher forgets faster
	K         float64       // alert above mean + K standard deviations
	MinCount  int           // and never below this many items per bucket
	MinFactor float64       // and never below MinFactor times the mean
	Warmup    int           // buckets to learn before alerting
	Ignore    Patterns
}

// DefaultSpikeConfig is tuned for a server where a busy origin produces tens
// of items per minute.
func DefaultSpikeConfig() SpikeConfig {
	return SpikeConfig{Bucket: time.Minute, Alpha: 0.05, K: 4, MinCount: 50, MinFactor: 3, Warmup: 30}
}

// maxGapBuckets bounds the zero buckets folded in after an origin goes quiet.
const maxGapBuckets = 1440

// idleExpiry removes origins that have not produced anything for a week.
const idleExpiry = 7 * 24 * time.Hour

// Spike learns the normal creation rate of each origin across all players
// (exponentially weighted mean and variance per bucket) and flags a bucket
// far above it. It catches what per-player limits miss: many accounts
// exploiting the same bug at once.
type Spike struct {
	cfg   SpikeConfig
	state spikeState
}

type spikeState struct {
	Watermark int64                  `json:"watermark"`
	Downtimes []span                 `json:"downtimes"`
	Origins   map[string]*originRate `json:"origins"`
}

// span is a closed range of buckets in which the whole ledger was silent.
type span struct {
	From int64 `json:"from"`
	To   int64 `json:"to"`
}

// downtimeMinBuckets is how long the entire ledger must stay silent to count
// as the server being down. Those buckets are not "zero items" for any origin:
// folding them in would drag every baseline down, and normal traffic after a
// restart or maintenance would then look like a spike.
const downtimeMinBuckets = 2

// keepDowntimes bounds the remembered downtime spans.
const keepDowntimes = 64

type originRate struct {
	Bucket  int64   `json:"bucket"`
	Count   int     `json:"count"`
	Mean    float64 `json:"mean"`
	Var     float64 `json:"var"`
	N       int     `json:"n"`
	Alerted bool    `json:"alerted"`
}

// NewSpike builds the rule; zero fields of cfg take the defaults.
func NewSpike(cfg SpikeConfig) *Spike {
	def := DefaultSpikeConfig()
	if cfg.Bucket <= 0 {
		cfg.Bucket = def.Bucket
	}
	if cfg.Alpha <= 0 || cfg.Alpha >= 1 {
		cfg.Alpha = def.Alpha
	}
	if cfg.K <= 0 {
		cfg.K = def.K
	}
	if cfg.MinCount <= 0 {
		cfg.MinCount = def.MinCount
	}
	if cfg.MinFactor <= 0 {
		cfg.MinFactor = def.MinFactor
	}
	if cfg.Warmup <= 0 {
		cfg.Warmup = def.Warmup
	}
	return &Spike{cfg: cfg, state: spikeState{Origins: map[string]*originRate{}}}
}

func (*Spike) Name() string { return "spike" }

func (s *Spike) Observe(ev ledger.Event) []Finding {
	bucketSec := int64(s.cfg.Bucket / time.Second)
	now := ev.CreatedAt.Unix()
	bucket := now / bucketSec
	// Every event, of any kind, proves the server was up: track global silence first.
	if now > s.state.Watermark {
		if s.state.Watermark > 0 {
			last := s.state.Watermark / bucketSec
			if bucket-last-1 >= downtimeMinBuckets {
				s.state.Downtimes = append(s.state.Downtimes, span{From: last + 1, To: bucket - 1})
				if n := len(s.state.Downtimes); n > keepDowntimes {
					s.state.Downtimes = s.state.Downtimes[n-keepDowntimes:]
				}
			}
		}
		s.state.Watermark = now
	}
	if ev.Kind != "birth" || s.cfg.Ignore.Match(ev.Origin) {
		return nil
	}
	origin := originKey(ev)

	r := s.state.Origins[origin]
	if r == nil {
		r = &originRate{Bucket: bucket}
		s.state.Origins[origin] = r
	}
	if bucket > r.Bucket {
		s.roll(r, bucket)
	}
	r.Count++

	if r.Alerted || r.N < s.cfg.Warmup || r.Count < s.cfg.MinCount {
		return nil
	}
	count := float64(r.Count)
	std := math.Sqrt(r.Var)
	if count <= r.Mean+s.cfg.K*std || count <= s.cfg.MinFactor*r.Mean {
		return nil
	}
	r.Alerted = true
	return []Finding{{
		Rule:     "spike",
		Severity: Warning,
		Key:      dedupKey("spike", origin, strconv.FormatInt(r.Bucket, 10)),
		At:       ev.CreatedAt,
		Count:    1,
		Origin:   origin,
		Facts: map[string]any{
			"count":  r.Count,
			"mean":   round2(r.Mean),
			"stddev": round2(std),
			"bucket": FormatDuration(s.cfg.Bucket),
		},
	}}
}

// roll closes the current bucket and every empty bucket up to next, except
// the ones where the server was down.
func (s *Spike) roll(r *originRate, next int64) {
	s.fold(r, float64(r.Count))
	quiet := next - r.Bucket - 1
	for _, d := range s.state.Downtimes {
		from, to := max(d.From, r.Bucket+1), min(d.To, next-1)
		if to >= from {
			quiet -= to - from + 1
		}
	}
	for gap := min(quiet, maxGapBuckets); gap > 0; gap-- {
		s.fold(r, 0)
	}
	r.Bucket, r.Count, r.Alerted = next, 0, false
}

// fold adds one observation to the exponentially weighted mean and variance.
func (s *Spike) fold(r *originRate, x float64) {
	if r.N == 0 {
		r.Mean, r.Var, r.N = x, 0, 1
		return
	}
	diff := x - r.Mean
	incr := s.cfg.Alpha * diff
	r.Mean += incr
	r.Var = (1 - s.cfg.Alpha) * (r.Var + diff*incr)
	r.N++
}

func round2(x float64) float64 { return math.Round(x*100) / 100 }

func (s *Spike) SaveState() (json.RawMessage, error) {
	idle := int64(idleExpiry/time.Second) / int64(s.cfg.Bucket/time.Second)
	current := s.state.Watermark / int64(s.cfg.Bucket/time.Second)
	for origin, r := range s.state.Origins {
		if current-r.Bucket > idle {
			delete(s.state.Origins, origin)
		}
	}
	return json.Marshal(s.state)
}

func (s *Spike) LoadState(raw json.RawMessage) error {
	st := spikeState{Origins: map[string]*originRate{}}
	if err := json.Unmarshal(raw, &st); err != nil {
		return err
	}
	if st.Origins == nil {
		st.Origins = map[string]*originRate{}
	}
	s.state = st
	return nil
}
