package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/victorcamposr/itemwatch/internal/detect"
	"github.com/victorcamposr/itemwatch/internal/queue"
	"github.com/victorcamposr/itemwatch/internal/render"
)

type fakeNotifier struct {
	min  detect.Severity
	err  error
	sent int
}

func (f *fakeNotifier) Name() string      { return "fake" }
func (f *fakeNotifier) MinSeverity() int8 { return int8(f.min) }
func (f *fakeNotifier) Notify(context.Context, queue.Notification) error {
	f.sent++
	return f.err
}

func TestDirect(t *testing.T) {
	n := queue.Notification{Severity: int8(detect.Warning)}
	broken := &fakeNotifier{min: detect.Info, err: errors.New("down")}
	working := &fakeNotifier{min: detect.Info}
	picky := &fakeNotifier{min: detect.Critical}

	if err := Direct(context.Background(), []Notifier{broken, picky, working}, n); err != nil {
		t.Fatalf("one channel took it: %v", err)
	}
	if working.sent != 1 || broken.sent != 1 || picky.sent != 0 {
		t.Fatalf("sent working=%d broken=%d picky=%d", working.sent, broken.sent, picky.sent)
	}
	if err := Direct(context.Background(), []Notifier{broken}, n); !errors.Is(err, broken.err) {
		t.Fatalf("err %v", err)
	}
	if err := Direct(context.Background(), nil, n); !errors.Is(err, ErrNoChannel) {
		t.Fatalf("no channels: %v", err)
	}
	if err := Direct(context.Background(), []Notifier{picky}, n); !errors.Is(err, ErrNoChannel) {
		t.Fatalf("nobody takes this severity: %v", err)
	}
}

func TestDiscordFooterOfDirectNotification(t *testing.T) {
	d := &Discord{Lang: render.English}
	body, err := d.Payload(queue.Notification{Rule: "probe", Severity: int8(detect.Critical), Title: "x"})
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Embeds []struct {
			Footer struct {
				Text string `json:"text"`
			} `json:"footer"`
		} `json:"embeds"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if text := got.Embeds[0].Footer.Text; strings.Contains(text, "#0") || !strings.Contains(text, "probe") {
		t.Fatalf("footer %q", text)
	}
}
