// Package config reads the service configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/victorcamposr/itemwatch/internal/detect"
	"github.com/victorcamposr/itemwatch/internal/render"
)

// Config is the full service configuration.
type Config struct {
	GameDSN  string
	StateDSN string
	RedisURL string

	HTTPAddr string
	APIToken string
	LogLevel string
	Lang     render.Lang

	LedgerTable  string
	BatchSize    int
	PollInterval time.Duration
	StartLatest  bool

	DupeScan     bool
	DupeInterval time.Duration
	DupeConfirm  time.Duration

	BurstLimits []detect.Limit
	Ignore      detect.Patterns
	Spike       detect.SpikeConfig
	StaffRule   bool

	DiscordWebhook string
	DiscordMin     detect.Severity

	Stream     string
	DeadLetter string
	Group      string
	Consumer   string
}

// DefaultIgnore lists origins that move or return existing value instead of
// creating it.
const DefaultIgnore = "world,selftest,change,bank,flask,stash,market,market:*,map"

// DefaultBurstLimits is a starting point; tune it to the server's economy.
const DefaultBurstLimits = "quest:=3/24h,task:=20/1h,boss:=60/1h,loot:=1500/10m,*=300/10m"

// Load reads the environment.
func Load() (Config, error) {
	var errs []error
	get := func(key, def string) string {
		if v, ok := os.LookupEnv("ITEMWATCH_" + key); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
		return def
	}
	duration := func(key, def string) time.Duration {
		d, err := time.ParseDuration(get(key, def))
		if err != nil || d <= 0 {
			errs = append(errs, fmt.Errorf("ITEMWATCH_%s: invalid duration", key))
		}
		return d
	}
	integer := func(key string, def int) int {
		n, err := strconv.Atoi(get(key, strconv.Itoa(def)))
		if err != nil || n <= 0 {
			errs = append(errs, fmt.Errorf("ITEMWATCH_%s: must be a positive integer", key))
		}
		return n
	}
	float := func(key string, def float64) float64 {
		f, err := strconv.ParseFloat(get(key, strconv.FormatFloat(def, 'f', -1, 64)), 64)
		if err != nil || f <= 0 {
			errs = append(errs, fmt.Errorf("ITEMWATCH_%s: must be a positive number", key))
		}
		return f
	}
	boolean := func(key string, def bool) bool {
		b, err := strconv.ParseBool(get(key, strconv.FormatBool(def)))
		if err != nil {
			errs = append(errs, fmt.Errorf("ITEMWATCH_%s: must be true or false", key))
		}
		return b
	}

	host, _ := os.Hostname()
	c := Config{
		GameDSN:        get("GAME_DSN", ""),
		StateDSN:       get("STATE_DSN", ""),
		RedisURL:       get("REDIS_URL", "redis://localhost:6379/0"),
		HTTPAddr:       get("HTTP_ADDR", ":8080"),
		APIToken:       get("API_TOKEN", ""),
		LogLevel:       get("LOG_LEVEL", "info"),
		Lang:           render.ParseLang(get("LANG", "en")),
		LedgerTable:    get("LEDGER_TABLE", "item_hash_log"),
		BatchSize:      integer("BATCH_SIZE", 2000),
		PollInterval:   duration("POLL_INTERVAL", "2s"),
		StartLatest:    get("START", "latest") != "earliest",
		DupeScan:       boolean("DUPE_SCAN", true),
		DupeInterval:   duration("DUPE_SCAN_INTERVAL", "2m"),
		DupeConfirm:    duration("DUPE_CONFIRM_AFTER", "20m"),
		Ignore:         detect.ParsePatterns(get("IGNORE_ORIGINS", DefaultIgnore)),
		StaffRule:      boolean("STAFF_RULE", true),
		DiscordWebhook: get("DISCORD_WEBHOOK_URL", ""),
		Stream:         get("STREAM", "itemwatch:alerts"),
		DeadLetter:     get("DEAD_LETTER_STREAM", "itemwatch:alerts:dead"),
		Group:          get("CONSUMER_GROUP", "notifiers"),
		Consumer:       get("CONSUMER_NAME", host),
	}
	c.Spike = detect.SpikeConfig{
		Bucket:    duration("SPIKE_BUCKET", "1m"),
		Alpha:     float("SPIKE_ALPHA", 0.05),
		K:         float("SPIKE_K", 4),
		MinCount:  integer("SPIKE_MIN_COUNT", 50),
		MinFactor: float("SPIKE_MIN_FACTOR", 3),
		Warmup:    integer("SPIKE_WARMUP", 30),
		Ignore:    append(detect.Patterns{"gm:"}, c.Ignore...),
	}
	if c.Spike.Bucket < time.Second {
		errs = append(errs, errors.New("ITEMWATCH_SPIKE_BUCKET: at least 1s"))
	}

	limits, err := detect.ParseLimits(get("BURST_LIMITS", DefaultBurstLimits))
	if err != nil {
		errs = append(errs, fmt.Errorf("ITEMWATCH_BURST_LIMITS: %w", err))
	}
	c.BurstLimits = limits

	if c.DiscordMin, err = detect.ParseSeverity(get("DISCORD_MIN_SEVERITY", "warning")); err != nil {
		errs = append(errs, fmt.Errorf("ITEMWATCH_DISCORD_MIN_SEVERITY: %w", err))
	}

	if c.GameDSN == "" {
		errs = append(errs, errors.New("ITEMWATCH_GAME_DSN is required"))
	} else if c.GameDSN, err = normalizeDSN(c.GameDSN); err != nil {
		errs = append(errs, fmt.Errorf("ITEMWATCH_GAME_DSN: %w", err))
	}
	if c.StateDSN == "" {
		errs = append(errs, errors.New("ITEMWATCH_STATE_DSN is required"))
	} else if c.StateDSN, err = normalizeDSN(c.StateDSN); err != nil {
		errs = append(errs, fmt.Errorf("ITEMWATCH_STATE_DSN: %w", err))
	}
	return c, errors.Join(errs...)
}

// normalizeDSN forces UTC time parsing, which the store relies on.
func normalizeDSN(dsn string) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", err
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params["time_zone"] = "'+00:00'"
	return cfg.FormatDSN(), nil
}
