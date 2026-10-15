package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/victorcamposr/itemwatch/internal/store"
)

type fakeStore struct {
	alerts map[int64]store.Alert
	filter store.Filter
}

func (f *fakeStore) ListAlerts(_ context.Context, filter store.Filter) ([]store.Alert, error) {
	f.filter = filter
	var out []store.Alert
	for _, a := range f.alerts {
		out = append(out, a)
	}
	return out, nil
}

func (f *fakeStore) GetAlert(_ context.Context, id int64) (store.Alert, error) {
	a, ok := f.alerts[id]
	if !ok {
		return a, store.ErrNotFound
	}
	return a, nil
}

func (f *fakeStore) Resolve(_ context.Context, id int64, by, note string) (store.Alert, error) {
	a, ok := f.alerts[id]
	if !ok {
		return a, store.ErrNotFound
	}
	if a.Status != "open" {
		return a, store.ErrConflict
	}
	a.Status, a.ResolvedBy, a.ResolutionNote = "resolved", by, note
	f.alerts[id] = a
	return a, nil
}

func (f *fakeStore) Reopen(_ context.Context, id int64) (store.Alert, error) {
	a := f.alerts[id]
	return a, nil
}

func (f *fakeStore) Stats(context.Context, string, func(int8) string) (store.Stats, error) {
	return store.Stats{Open: map[string]int64{"critical": 1}, OpenTotal: 1}, nil
}

func newTestServer(token string) (*fakeStore, http.Handler) {
	fs := &fakeStore{alerts: map[int64]store.Alert{
		1: {ID: 1, Rule: "dupe", Severity: 3, Status: "open", Title: "dupe"},
	}}
	s := &Server{Store: fs, Token: token, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	return fs, s.Handler()
}

func do(h http.Handler, method, path, token, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestAuth(t *testing.T) {
	_, h := newTestServer("secret")
	if rec := do(h, "GET", "/v1/alerts", "", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token: %d", rec.Code)
	}
	if rec := do(h, "GET", "/v1/alerts", "wrong", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token: %d", rec.Code)
	}
	if rec := do(h, "GET", "/v1/alerts", "secret", ""); rec.Code != http.StatusOK {
		t.Fatalf("good token: %d", rec.Code)
	}
	if rec := do(h, "GET", "/healthz", "", ""); rec.Code != http.StatusOK {
		t.Fatalf("healthz must stay open: %d", rec.Code)
	}
}

func TestListFilters(t *testing.T) {
	fs, h := newTestServer("")
	rec := do(h, "GET", "/v1/alerts?status=open&min_severity=warning&limit=10&before_id=99", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	if fs.filter.Status != "open" || fs.filter.MinSeverity != 2 || fs.filter.Limit != 10 || fs.filter.BeforeID != 99 {
		t.Fatalf("filter %+v", fs.filter)
	}
	var body struct {
		Alerts []struct {
			Severity string `json:"severity"`
		} `json:"alerts"`
	}
	json.Unmarshal(rec.Body.Bytes(), &body)
	if body.Alerts[0].Severity != "critical" {
		t.Fatalf("severity name missing: %s", rec.Body)
	}
	if rec := do(h, "GET", "/v1/alerts?status=bogus", "", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad status accepted: %d", rec.Code)
	}
}

func TestResolveFlow(t *testing.T) {
	_, h := newTestServer("")
	if rec := do(h, "POST", "/v1/alerts/1/resolve", "", `{"note":"x"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("missing by accepted: %d", rec.Code)
	}
	if rec := do(h, "POST", "/v1/alerts/1/resolve", "", `{"by":"staff","note":"banned"}`); rec.Code != http.StatusOK {
		t.Fatalf("resolve: %d %s", rec.Code, rec.Body)
	}
	if rec := do(h, "POST", "/v1/alerts/1/resolve", "", `{"by":"staff"}`); rec.Code != http.StatusConflict {
		t.Fatalf("second resolve: %d", rec.Code)
	}
	if rec := do(h, "POST", "/v1/alerts/9/resolve", "", `{"by":"staff"}`); rec.Code != http.StatusNotFound {
		t.Fatalf("missing alert: %d", rec.Code)
	}
	if rec := do(h, "GET", "/v1/alerts/abc", "", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad id: %d", rec.Code)
	}
}
