package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/victorcamposr/itemwatch/internal/detect"
	"github.com/victorcamposr/itemwatch/internal/queue"
	"github.com/victorcamposr/itemwatch/internal/render"
)

var sample = queue.Notification{
	AlertID: 42, Generation: 1, Rule: "dupe", Severity: int8(detect.Critical),
	Title: "Item 00aa is in 2 places at once", PlayerName: "Knight", ItemHash: "00aa",
	Occurrences: 3, FirstSeen: time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC),
}

func TestDiscordPayload(t *testing.T) {
	d := &Discord{Lang: render.Portuguese}
	body, err := d.Payload(sample)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Embeds []struct {
			Title       string `json:"title"`
			Description string `json:"description"`
			Color       int    `json:"color"`
			Fields      []struct{ Name, Value string }
		} `json:"embeds"`
		AllowedMentions struct {
			Parse []string `json:"parse"`
		} `json:"allowed_mentions"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	e := got.Embeds[0]
	if e.Title != sample.Title || e.Color != 0xDA373C {
		t.Fatalf("embed %+v", e)
	}
	if e.Description != "Reaberto depois de resolvido" {
		t.Fatalf("reopened alert not marked: %q", e.Description)
	}
	if got.AllowedMentions.Parse == nil || len(got.AllowedMentions.Parse) != 0 {
		t.Fatal("mentions must be disabled")
	}
	if !strings.Contains(string(body), "Jogador") {
		t.Fatal("labels not localized")
	}
}

func TestDiscordStatusHandling(t *testing.T) {
	cases := []struct {
		status    int
		wantErr   bool
		permanent bool
	}{
		{http.StatusNoContent, false, false},
		{http.StatusTooManyRequests, true, false},
		{http.StatusBadGateway, true, false},
		{http.StatusNotFound, true, true},
	}
	for _, c := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(c.status)
		}))
		err := (&Discord{URL: srv.URL}).Notify(context.Background(), sample)
		srv.Close()
		if (err != nil) != c.wantErr {
			t.Fatalf("status %d: err = %v", c.status, err)
		}
		var p *PermanentError
		if errors.As(err, &p) != c.permanent {
			t.Fatalf("status %d: permanent = %v, want %v", c.status, !c.permanent, c.permanent)
		}
	}
}

func TestBackoffDoublesUpToCap(t *testing.T) {
	d := New(nil, Config{BaseBackoff: 10 * time.Second, MaxBackoff: time.Minute}, nil, nil, nil)
	want := []time.Duration{10 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second, time.Minute, time.Minute}
	for n, w := range want {
		if got := d.Backoff(int64(n)); got != w {
			t.Errorf("Backoff(%d) = %v, want %v", n, got, w)
		}
	}
}
