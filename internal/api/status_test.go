package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/victorcamposr/itemwatch/internal/probe"
	"github.com/victorcamposr/itemwatch/internal/watcher"
)

func statusHandler(token string, rt *Runtime) http.Handler {
	s := &Server{Store: &fakeStore{}, Token: token, Runtime: rt, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	return s.Handler()
}

func healthyRuntime() *Runtime {
	read := time.Date(2026, 10, 15, 12, 0, 1, 0, time.UTC)
	scan := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	since := time.Date(2026, 10, 15, 11, 59, 0, 0, time.UTC)
	return &Runtime{
		Version:   "1.2.3",
		StartedAt: time.Date(2026, 10, 15, 11, 0, 0, 0, time.UTC),
		Leader:    func() bool { return true },
		Watcher: func() watcher.State {
			return watcher.State{Tailing: true, Cursor: 90, LastReadAt: &read, DupeScan: true, DupeScanAt: &scan}
		},
		Cursor: func(context.Context) (uint64, error) { return 0, errors.New("must not be asked while tailing") },
		Head:   func(context.Context) (uint64, error) { return 100, nil },
		Queue:  func(context.Context) (int64, int64) { return 3, 1 },
		Probes: func() []probe.Status {
			return []probe.Status{{Name: "ot-status", FailingSince: &since, LastError: "refused", MS: 12, Alert: probe.AlertNone}}
		},
	}
}

func getStatus(t *testing.T, h http.Handler) map[string]any {
	t.Helper()
	rec := do(h, "GET", "/v1/status", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return body
}

func TestStatusRequiresToken(t *testing.T) {
	h := statusHandler("secret", healthyRuntime())
	if rec := do(h, "GET", "/v1/status", "", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token: %d", rec.Code)
	}
	if rec := do(h, "GET", "/v1/status", "secret", ""); rec.Code != http.StatusOK {
		t.Fatalf("good token: %d", rec.Code)
	}
}

func TestStatusReport(t *testing.T) {
	body := getStatus(t, statusHandler("", healthyRuntime()))
	if body["version"] != "1.2.3" || body["started_at"] != "2026-10-15T11:00:00Z" || body["leader"] != true {
		t.Fatalf("top level: %v", body)
	}
	ledger := body["ledger"].(map[string]any)
	if ledger["cursor"] != 90.0 || ledger["head"] != 100.0 || ledger["lag"] != 10.0 || ledger["last_read_at"] != "2026-10-15T12:00:01Z" {
		t.Fatalf("ledger: %v", ledger)
	}
	dupes := body["dupe_scan"].(map[string]any)
	if dupes["enabled"] != true || dupes["last_at"] != "2026-10-15T12:00:00Z" {
		t.Fatalf("dupe scan: %v", dupes)
	}
	queue := body["queue"].(map[string]any)
	if queue["pending"] != 3.0 || queue["dead_letter"] != 1.0 {
		t.Fatalf("queue: %v", queue)
	}
	p := body["probes"].([]any)[0].(map[string]any)
	if p["name"] != "ot-status" || p["ok"] != false || p["failing_since"] != "2026-10-15T11:59:00Z" ||
		p["last_ok_at"] != nil || p["last_error"] != "refused" || p["ms"] != 12.0 || p["alert"] != "none" {
		t.Fatalf("probe: %v", p)
	}
	if _, ok := p["facts"]; ok {
		t.Fatalf("empty facts must be omitted: %v", p)
	}
}

func TestStatusSurvivesEveryDependency(t *testing.T) {
	rt := healthyRuntime()
	rt.Leader = func() bool { return false }
	rt.Watcher = func() watcher.State { return watcher.State{} }
	rt.Cursor = func(context.Context) (uint64, error) { return 0, errors.New("state database down") }
	rt.Head = func(context.Context) (uint64, error) { return 0, errors.New("game database down") }
	rt.Queue = func(context.Context) (int64, int64) { return -1, -1 }
	rt.Probes = func() []probe.Status { return []probe.Status{} }

	body := getStatus(t, statusHandler("", rt))
	ledger := body["ledger"].(map[string]any)
	if ledger["cursor"] != -1.0 || ledger["head"] != -1.0 || ledger["lag"] != -1.0 || ledger["last_read_at"] != nil {
		t.Fatalf("ledger: %v", ledger)
	}
	queue := body["queue"].(map[string]any)
	if queue["pending"] != -1.0 || queue["dead_letter"] != -1.0 {
		t.Fatalf("queue: %v", queue)
	}
	dupes := body["dupe_scan"].(map[string]any)
	if dupes["enabled"] != false || dupes["last_at"] != nil {
		t.Fatalf("dupe scan: %v", dupes)
	}
	if p, ok := body["probes"].([]any); !ok || len(p) != 0 {
		t.Fatalf("probes must be an empty list: %v", body["probes"])
	}
}

func TestStatusOnAReplicaNotTailing(t *testing.T) {
	rt := healthyRuntime()
	rt.Watcher = func() watcher.State { return watcher.State{DupeScan: true} }
	rt.Cursor = func(context.Context) (uint64, error) { return 40, nil }
	ledger := getStatus(t, statusHandler("", rt))["ledger"].(map[string]any)
	if ledger["cursor"] != 40.0 || ledger["lag"] != 60.0 {
		t.Fatalf("ledger: %v", ledger)
	}
}

func TestStatusLagIsNeverNegative(t *testing.T) {
	rt := healthyRuntime()
	rt.Head = func(context.Context) (uint64, error) { return 50, nil } // the ledger was truncated
	if lag := getStatus(t, statusHandler("", rt))["ledger"].(map[string]any)["lag"]; lag != 0.0 {
		t.Fatalf("lag %v", lag)
	}
}

func TestStatusDoesNotWaitForAHungDependency(t *testing.T) {
	rt := healthyRuntime()
	rt.Head = func(ctx context.Context) (uint64, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	start := time.Now()
	body := getStatus(t, statusHandler("", rt))
	if took := time.Since(start); took > statusTimeout+time.Second {
		t.Fatalf("took %s", took)
	}
	if body["ledger"].(map[string]any)["head"] != -1.0 || body["queue"].(map[string]any)["pending"] != 3.0 {
		t.Fatalf("%v", body)
	}
}

func TestStatusRouteIsOptional(t *testing.T) {
	if rec := do(statusHandler("", nil), "GET", "/v1/status", "", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("code %d", rec.Code)
	}
}
