// Command itemwatch watches a game server's item ledger for duplicated items
// and abnormal item creation, and announces what it finds.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"

	"github.com/victorcamposr/itemwatch/internal/api"
	"github.com/victorcamposr/itemwatch/internal/config"
	"github.com/victorcamposr/itemwatch/internal/detect"
	"github.com/victorcamposr/itemwatch/internal/dispatch"
	"github.com/victorcamposr/itemwatch/internal/ledger"
	"github.com/victorcamposr/itemwatch/internal/metrics"
	"github.com/victorcamposr/itemwatch/internal/queue"
	"github.com/victorcamposr/itemwatch/internal/store"
	"github.com/victorcamposr/itemwatch/internal/watcher"
)

var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck())
	}
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println(version)
		return
	}

	cfg, err := config.Load()
	log := newLogger(cfg.LogLevel)
	if err != nil {
		log.Error("invalid configuration", "err", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("itemwatch stopped", "err", err)
		os.Exit(1)
	}
	log.Info("itemwatch stopped")
}

func run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	gameDB, err := openDB(ctx, cfg.GameDSN, 4)
	if err != nil {
		return fmt.Errorf("game database: %w", err)
	}
	defer gameDB.Close()
	stateDB, err := openDB(ctx, cfg.StateDSN, 8)
	if err != nil {
		return fmt.Errorf("state database: %w", err)
	}
	defer stateDB.Close()

	redisOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return fmt.Errorf("redis url: %w", err)
	}
	rdb := redis.NewClient(redisOpts)
	defer rdb.Close()

	st := store.New(stateDB)
	if err := st.Migrate(ctx); err != nil {
		return err
	}
	reader, err := ledger.NewReader(gameDB, cfg.LedgerTable, nil)
	if err != nil {
		return err
	}
	m := metrics.New()

	rules := []detect.Rule{
		detect.Unhashed{},
		detect.NewBurst(cfg.BurstLimits, append(detect.Patterns{"gm:"}, cfg.Ignore...)),
		detect.NewSpike(cfg.Spike),
	}
	if cfg.StaffRule {
		rules = append(rules, detect.StaffCreation{})
	}
	w := &watcher.Watcher{
		Ledger:       reader,
		Store:        st,
		Publisher:    queue.NewPublisher(rdb, cfg.Stream, 10000),
		Rules:        rules,
		Lang:         cfg.Lang,
		BatchSize:    cfg.BatchSize,
		PollInterval: cfg.PollInterval,
		DupeInterval: cfg.DupeInterval,
		StartLatest:  cfg.StartLatest,
		Log:          log,
		Metrics:      m,
	}
	if cfg.DupeScan {
		w.Dupes = detect.NewDupeTracker(cfg.DupeConfirm)
	}

	var notifiers []dispatch.Notifier
	if cfg.DiscordWebhook != "" {
		notifiers = append(notifiers, &dispatch.Discord{URL: cfg.DiscordWebhook, Min: cfg.DiscordMin, Lang: cfg.Lang})
	} else {
		log.Warn("no notification channel configured; alerts are only available through the API")
	}

	srv := &http.Server{
		Addr: cfg.HTTPAddr,
		Handler: (&api.Server{
			Store: st, Token: cfg.APIToken, Checkpoint: watcher.CheckpointName, Gatherer: m.Registry, Log: log,
			Ready: func(ctx context.Context) error {
				if err := st.Ping(ctx); err != nil {
					return fmt.Errorf("state database: %w", err)
				}
				if err := reader.Ping(ctx); err != nil {
					return fmt.Errorf("game database: %w", err)
				}
				return rdb.Ping(ctx).Err()
			},
		}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		log.Info("http listening", "addr", cfg.HTTPAddr, "version", version)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdown)
	})
	if len(notifiers) > 0 {
		g.Go(func() error {
			return dispatch.New(rdb, dispatch.Config{
				Stream: cfg.Stream, DeadLetter: cfg.DeadLetter, Group: cfg.Group, Consumer: cfg.Consumer,
			}, notifiers, log, m).Run(ctx)
		})
	}
	g.Go(func() error {
		return st.RunAsLeader(ctx, "itemwatch:ledger", 10*time.Second, func(leader bool) {
			if leader {
				m.Leader.Set(1)
				log.Info("leader lock acquired")
			} else {
				m.Leader.Set(0)
			}
		}, w.Run)
	})
	return g.Wait()
}

func openDB(ctx context.Context, dsn string, maxOpen int) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxOpen)
	db.SetConnMaxLifetime(30 * time.Minute)
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}

// healthcheck lets a distroless image check itself without curl.
func healthcheck() int {
	addr := os.Getenv("ITEMWATCH_HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	if addr[0] == ':' {
		addr = "127.0.0.1" + addr
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://" + addr + "/healthz")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
