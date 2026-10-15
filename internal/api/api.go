// Package api serves the alerts over HTTP for dashboards and staff tools.
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/victorcamposr/itemwatch/internal/detect"
	"github.com/victorcamposr/itemwatch/internal/store"
)

// Store is the subset of the store the API needs.
type Store interface {
	ListAlerts(ctx context.Context, f store.Filter) ([]store.Alert, error)
	GetAlert(ctx context.Context, id int64) (store.Alert, error)
	Resolve(ctx context.Context, id int64, by, note string) (store.Alert, error)
	Reopen(ctx context.Context, id int64) (store.Alert, error)
	Stats(ctx context.Context, checkpoint string, severityName func(int8) string) (store.Stats, error)
}

// Server holds the handlers.
type Server struct {
	Store      Store
	Token      string // bearer token for /v1; empty disables auth
	Checkpoint string
	Ready      func(ctx context.Context) error
	Gatherer   prometheus.Gatherer
	Log        *slog.Logger
}

// Handler returns the routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /readyz", s.ready)
	if s.Gatherer != nil {
		mux.Handle("GET /metrics", promhttp.HandlerFor(s.Gatherer, promhttp.HandlerOpts{}))
	}
	mux.Handle("GET /v1/alerts", s.auth(s.listAlerts))
	mux.Handle("GET /v1/alerts/{id}", s.auth(s.getAlert))
	mux.Handle("POST /v1/alerts/{id}/resolve", s.auth(s.resolve))
	mux.Handle("POST /v1/alerts/{id}/reopen", s.auth(s.reopen))
	mux.Handle("GET /v1/stats", s.auth(s.stats))
	return s.logRequests(mux)
}

func (s *Server) auth(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.Token != "" {
			got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !ok || subtle.ConstantTimeCompare([]byte(got), []byte(s.Token)) != 1 {
				writeError(w, http.StatusUnauthorized, "unauthorized")
				return
			}
		}
		next(w, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			if rec := recover(); rec != nil {
				s.Log.Error("panic in handler", "path", r.URL.Path, "panic", rec)
				writeError(sw, http.StatusInternalServerError, "internal error")
			}
			if r.URL.Path != "/healthz" && r.URL.Path != "/metrics" {
				s.Log.Debug("http", "method", r.Method, "path", r.URL.Path, "status", sw.status, "took", time.Since(start))
			}
		}()
		next.ServeHTTP(sw, r)
	})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if s.Ready != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := s.Ready(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
	}
	w.Write([]byte("ready\n"))
}

func (s *Server) listAlerts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.Filter{Status: q.Get("status"), Rule: q.Get("rule")}
	if f.Status != "" && f.Status != "open" && f.Status != "resolved" {
		writeError(w, http.StatusBadRequest, "status must be open or resolved")
		return
	}
	if v := q.Get("min_severity"); v != "" {
		sev, err := detect.ParseSeverity(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		f.MinSeverity = int8(sev)
	}
	var err error
	if f.Limit, err = intParam(q.Get("limit")); err != nil {
		writeError(w, http.StatusBadRequest, "limit must be a number")
		return
	}
	before, err := intParam(q.Get("before_id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "before_id must be a number")
		return
	}
	f.BeforeID = int64(before)
	alerts, err := s.Store.ListAlerts(r.Context(), f)
	if err != nil {
		s.fail(w, err)
		return
	}
	for i := range alerts {
		named(&alerts[i])
	}
	writeJSON(w, http.StatusOK, map[string]any{"alerts": alerts})
}

func (s *Server) getAlert(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	a, err := s.Store.GetAlert(r.Context(), id)
	if err != nil {
		s.fail(w, err)
		return
	}
	named(&a)
	writeJSON(w, http.StatusOK, a)
}

type resolveBody struct {
	By   string `json:"by"`
	Note string `json:"note"`
}

func (s *Server) resolve(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var body resolveBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "body must be JSON with by and note")
		return
	}
	if strings.TrimSpace(body.By) == "" {
		writeError(w, http.StatusBadRequest, "by is required")
		return
	}
	a, err := s.Store.Resolve(r.Context(), id, strings.TrimSpace(body.By), strings.TrimSpace(body.Note))
	s.transition(w, a, err, "alert is not open")
}

func (s *Server) reopen(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	a, err := s.Store.Reopen(r.Context(), id)
	s.transition(w, a, err, "alert is not resolved")
}

func (s *Server) transition(w http.ResponseWriter, a store.Alert, err error, conflict string) {
	if errors.Is(err, store.ErrConflict) {
		named(&a)
		writeJSON(w, http.StatusConflict, map[string]any{"error": conflict, "alert": a})
		return
	}
	if err != nil {
		s.fail(w, err)
		return
	}
	named(&a)
	writeJSON(w, http.StatusOK, a)
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	st, err := s.Store.Stats(r.Context(), s.Checkpoint, func(v int8) string { return detect.Severity(v).String() })
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) fail(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "alert not found")
		return
	}
	s.Log.Error("api", "err", err)
	writeError(w, http.StatusInternalServerError, "internal error")
}

func named(a *store.Alert) { a.SeverityName = detect.Severity(a.Severity).String() }

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, "invalid alert id")
		return 0, false
	}
	return id, true
}

func intParam(v string) (int, error) {
	if v == "" {
		return 0, nil
	}
	return strconv.Atoi(v)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
