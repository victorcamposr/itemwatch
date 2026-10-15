//go:build integration

// Package itest runs the service against real MariaDB and Redis containers.
// Run with: go test -tags integration ./internal/itest/
package itest

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go/modules/mariadb"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/victorcamposr/itemwatch/internal/detect"
	"github.com/victorcamposr/itemwatch/internal/dispatch"
	"github.com/victorcamposr/itemwatch/internal/ledger"
	"github.com/victorcamposr/itemwatch/internal/metrics"
	"github.com/victorcamposr/itemwatch/internal/queue"
	"github.com/victorcamposr/itemwatch/internal/render"
	"github.com/victorcamposr/itemwatch/internal/store"
	"github.com/victorcamposr/itemwatch/internal/watcher"
)

var (
	gameDB  *sql.DB
	stateDB *sql.DB
	rdb     *redis.Client
	quiet   = slog.New(slog.NewTextHandler(io.Discard, nil))
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	db, err := mariadb.Run(ctx, "mariadb:10.11",
		mariadb.WithDatabase("game"), mariadb.WithUsername("root"), mariadb.WithPassword("test"),
		mariadb.WithScripts("../../testdata/game_schema.sql"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "mariadb:", err)
		os.Exit(1)
	}
	rc, err := tcredis.Run(ctx, "redis:7.4-alpine")
	if err != nil {
		fmt.Fprintln(os.Stderr, "redis:", err)
		os.Exit(1)
	}

	dsn, _ := db.ConnectionString(ctx, "parseTime=true", "loc=UTC", "multiStatements=false")
	gameDB = mustOpen(dsn)
	if _, err := gameDB.Exec("CREATE DATABASE itemwatch"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	stateDB = mustOpen(strings.Replace(dsn, "/game?", "/itemwatch?", 1))
	url, _ := rc.ConnectionString(ctx)
	opts, _ := redis.ParseURL(url)
	rdb = redis.NewClient(opts)

	code := m.Run()
	_ = db.Terminate(ctx)
	_ = rc.Terminate(ctx)
	os.Exit(code)
}

func mustOpen(dsn string) *sql.DB {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	if err := db.Ping(); err != nil {
		panic(err)
	}
	return db
}

func exec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func reset(t *testing.T) {
	t.Helper()
	for _, table := range []string{"players", "item_hash_log", "player_items", "player_depotitems", "player_inboxitems",
		"player_rewards", "market_offers", "market_offer_items"} {
		exec(t, gameDB, "DELETE FROM "+table)
	}
	for _, table := range []string{"alerts", "checkpoints"} {
		stateDB.Exec("DELETE FROM " + table)
	}
	if err := rdb.FlushAll(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
}

func logRow(t *testing.T, hash, event, origin string, player int64, at time.Time) {
	exec(t, gameDB, "INSERT INTO item_hash_log (hash, event, itemtype, origin, player_id, created_at) VALUES (?, ?, 3031, ?, ?, ?)",
		hash, event, origin, player, at.Unix())
}

type fixture struct {
	store  *store.Store
	reader *ledger.Reader
}

func setup(t *testing.T) fixture {
	t.Helper()
	reset(t)
	st := store.New(stateDB)
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	reader, err := ledger.NewReader(gameDB, "item_hash_log", nil)
	if err != nil {
		t.Fatal(err)
	}
	return fixture{store: st, reader: reader}
}

func (f fixture) newWatcher(t *testing.T) *watcher.Watcher {
	limits, err := detect.ParseLimits("quest:=2/24h,*=1000/10m")
	if err != nil {
		t.Fatal(err)
	}
	return &watcher.Watcher{
		Ledger:    f.reader,
		Store:     f.store,
		Publisher: queue.NewPublisher(rdb, "itemwatch:alerts", 1000),
		Rules:     []detect.Rule{detect.Unhashed{}, detect.NewBurst(limits, nil), detect.StaffCreation{}},
		Dupes:     detect.NewDupeTracker(0),
		Lang:      render.English,
		BatchSize: 100,
		Log:       quiet,
		Metrics:   metrics.New(),
	}
}

// restored mimics a process restart: fresh rules, state from the checkpoint.
func restored(t *testing.T, f fixture) (*watcher.Watcher, uint64) {
	w := f.newWatcher(t)
	cp, ok, err := f.store.LoadCheckpoint(context.Background(), watcher.CheckpointName)
	if err != nil || !ok {
		t.Fatalf("checkpoint: ok=%v err=%v", ok, err)
	}
	for _, r := range w.Rules {
		if s, ok := r.(detect.Stateful); ok {
			if raw, saved := cp.RuleState[r.Name()]; saved {
				if err := s.LoadState(raw); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	return w, cp.Cursor
}

func alertsByRule(t *testing.T, st *store.Store) map[string]store.Alert {
	t.Helper()
	list, err := st.ListAlerts(context.Background(), store.Filter{Limit: 200})
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]store.Alert{}
	for _, a := range list {
		out[a.Rule] = a
	}
	return out
}

func TestEndToEnd(t *testing.T) {
	ctx := context.Background()
	f := setup(t)
	now := time.Now().UTC()

	exec(t, gameDB, "INSERT INTO players (id, name) VALUES (1, 'Knight'), (2, 'Druid')")
	logRow(t, "0000000000000001", "unhashed", "script", 0, now)
	for i := range 3 {
		logRow(t, fmt.Sprintf("00000000000001%02d", i), "birth", "quest:Annihilator", 1, now)
	}
	logRow(t, "0000000000000200", "birth", "gm:Admin /i 3031", 2, now)
	exec(t, gameDB, "INSERT INTO player_items (player_id, sid, itemtype, hash) VALUES (1, 1, 3031, 'dddddddddddddddd')")
	exec(t, gameDB, "INSERT INTO player_depotitems (player_id, sid, itemtype, hash) VALUES (2, 1, 3031, 'dddddddddddddddd')")

	w := f.newWatcher(t)
	cursor, n, err := w.Step(ctx, 0)
	if err != nil || n != 5 {
		t.Fatalf("step: n=%d err=%v", n, err)
	}
	if err := w.ScanDupesOnce(ctx); err != nil {
		t.Fatal(err)
	}

	got := alertsByRule(t, f.store)
	for _, rule := range []string{"unhashed", "burst", "staff", "dupe"} {
		if _, ok := got[rule]; !ok {
			t.Errorf("missing %s alert; have %v", rule, keys(got))
		}
	}
	if b := got["burst"]; b.PlayerName != "Knight" || b.Severity != int8(detect.Warning) {
		t.Errorf("burst alert %+v", b)
	}
	if d := got["dupe"]; !strings.Contains(string(d.Details), `"player_name":"Druid"`) {
		t.Errorf("dupe holders not named: %s", d.Details)
	}

	// Every alert reached the stream exactly once.
	if l := rdb.XLen(ctx, "itemwatch:alerts").Val(); l != int64(len(got)) {
		t.Fatalf("stream length %d, want %d", l, len(got))
	}
	if pending, _ := f.store.Unpublished(ctx, 10); len(pending) != 0 {
		t.Fatalf("%d alerts left unpublished", len(pending))
	}

	// Restart: the checkpoint brings back cursor and rule state, so a fourth
	// reward inside the same window is not a new incident.
	w2, restoredCursor := restored(t, f)
	if restoredCursor != cursor {
		t.Fatalf("restored cursor %d, want %d", restoredCursor, cursor)
	}
	logRow(t, "0000000000000199", "birth", "quest:Annihilator", 1, now)
	if _, n, err := w2.Step(ctx, restoredCursor); err != nil || n != 1 {
		t.Fatalf("second step: n=%d err=%v", n, err)
	}
	if after := alertsByRule(t, f.store); len(after) != len(got) {
		t.Fatalf("restart produced new alerts: %v", keys(after))
	}
	if l := rdb.XLen(ctx, "itemwatch:alerts").Val(); l != int64(len(got)) {
		t.Fatalf("restart republished: stream length %d", l)
	}
}

func TestResolvedIncidentReopens(t *testing.T) {
	ctx := context.Background()
	f := setup(t)
	exec(t, gameDB, "INSERT INTO player_items (player_id, sid, itemtype, hash) VALUES (1, 1, 3031, 'eeeeeeeeeeeeeeee'), (1, 2, 3031, 'eeeeeeeeeeeeeeee')")

	w := f.newWatcher(t)
	if err := w.ScanDupesOnce(ctx); err != nil {
		t.Fatal(err)
	}
	dupe := alertsByRule(t, f.store)["dupe"]
	if _, err := f.store.Resolve(ctx, dupe.ID, "staff", "looked fine"); err != nil {
		t.Fatal(err)
	}

	// The copies are still there after a restart: the incident comes back.
	w2 := f.newWatcher(t)
	if err := w2.ScanDupesOnce(ctx); err != nil {
		t.Fatal(err)
	}
	again, err := f.store.GetAlert(ctx, dupe.ID)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != "open" || again.Generation != 1 || again.Occurrences != 2 {
		t.Fatalf("not reopened: %+v", again)
	}
	if l := rdb.XLen(ctx, "itemwatch:alerts").Val(); l != 2 {
		t.Fatalf("reopen not published: stream length %d", l)
	}
}

func TestDispatcherRetriesAndDeduplicates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	reset(t)

	var calls, failures atomic.Int32
	failures.Store(2)
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if strings.Contains(readAll(r), "#13 ") {
			w.WriteHeader(http.StatusNotFound) // revoked webhook: permanent
			return
		}
		if failures.Add(-1) >= 0 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer hook.Close()

	d := dispatch.New(rdb, dispatch.Config{
		Stream: "itemwatch:alerts", DeadLetter: "itemwatch:alerts:dead", Group: "notifiers", Consumer: "test",
		BaseBackoff: 100 * time.Millisecond, MaxBackoff: time.Second, RetryEvery: 50 * time.Millisecond,
		Block: 100 * time.Millisecond,
	}, []dispatch.Notifier{&dispatch.Discord{URL: hook.URL, Min: detect.Info}}, quiet, nil)
	go d.Run(ctx)

	pub := queue.NewPublisher(rdb, "itemwatch:alerts", 1000)
	n := queue.Notification{AlertID: 12, Rule: "dupe", Severity: int8(detect.Critical), Title: "x", FirstSeen: time.Now()}
	if _, err := pub.Publish(ctx, n); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "delivery after two failures", func() bool { return calls.Load() == 3 && pendingCount(ctx) == 0 })

	// The same generation published again (crash before marking it) is acked
	// without a second post.
	if _, err := pub.Publish(ctx, n); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "duplicate acked", func() bool { return pendingCount(ctx) == 0 && rdb.XLen(ctx, "itemwatch:alerts").Val() == 2 })
	time.Sleep(300 * time.Millisecond)
	if c := calls.Load(); c != 3 {
		t.Fatalf("duplicate was posted again: %d calls", c)
	}

	// A permanent rejection goes to the dead-letter stream.
	if _, err := pub.Publish(ctx, queue.Notification{AlertID: 13, Rule: "dupe", Severity: 3, Title: "y", FirstSeen: time.Now()}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "dead letter", func() bool { return rdb.XLen(ctx, "itemwatch:alerts:dead").Val() == 1 && pendingCount(ctx) == 0 })
}

func TestLeaderLock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	st := store.New(stateDB)

	aCtx, stopA := context.WithCancel(ctx)
	aRunning := make(chan struct{})
	go st.RunAsLeader(aCtx, "itemwatch:test", 100*time.Millisecond, func(bool) {}, func(c context.Context) error {
		close(aRunning)
		<-c.Done()
		return nil
	})
	<-aRunning

	var bRan atomic.Bool
	go st.RunAsLeader(ctx, "itemwatch:test", 100*time.Millisecond, func(bool) {}, func(c context.Context) error {
		bRan.Store(true)
		<-c.Done()
		return nil
	})
	time.Sleep(500 * time.Millisecond)
	if bRan.Load() {
		t.Fatal("second replica ran while the first held the lock")
	}
	stopA()
	waitFor(t, ctx, "standby takeover", bRan.Load)
}

func pendingCount(ctx context.Context) int64 {
	p, err := rdb.XPending(ctx, "itemwatch:alerts", "notifiers").Result()
	if err != nil {
		return -1
	}
	return p.Count
}

func waitFor(t *testing.T, ctx context.Context, what string, cond func() bool) {
	t.Helper()
	for !cond() {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func readAll(r *http.Request) string {
	b, _ := io.ReadAll(r.Body)
	return string(b)
}

func keys(m map[string]store.Alert) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}
