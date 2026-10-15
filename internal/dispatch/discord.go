package dispatch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/victorcamposr/itemwatch/internal/detect"
	"github.com/victorcamposr/itemwatch/internal/queue"
	"github.com/victorcamposr/itemwatch/internal/render"
)

// Discord posts notifications to a channel webhook as embeds.
type Discord struct {
	URL      string
	Min      detect.Severity
	Lang     render.Lang
	Username string
	Client   *http.Client
}

func (d *Discord) Name() string      { return "discord" }
func (d *Discord) MinSeverity() int8 { return int8(d.Min) }

var severityColor = map[detect.Severity]int{
	detect.Info:     0x5865F2,
	detect.Warning:  0xF0B232,
	detect.Critical: 0xDA373C,
}

type discordField struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline"`
}

type discordEmbed struct {
	Title       string         `json:"title"`
	Description string         `json:"description,omitempty"`
	Color       int            `json:"color"`
	Fields      []discordField `json:"fields,omitempty"`
	Timestamp   string         `json:"timestamp"`
	Footer      map[string]any `json:"footer"`
}

// Payload builds the webhook body. Exported for tests.
func (d *Discord) Payload(n queue.Notification) ([]byte, error) {
	sev := detect.Severity(n.Severity)
	label := func(k string) string { return render.Label(d.Lang, k) }
	var fields []discordField
	add := func(name, value string) {
		if value != "" {
			fields = append(fields, discordField{Name: label(name), Value: value, Inline: true})
		}
	}
	add("severity", label(sev.String()))
	add("player", n.PlayerName)
	add("origin", n.Origin)
	if n.ItemHash != "" {
		add("hash", "`"+n.ItemHash+"`")
	}
	if n.Occurrences > 1 {
		add("occurrences", fmt.Sprint(n.Occurrences))
	}
	embed := discordEmbed{
		Title:     n.Title,
		Color:     severityColor[sev],
		Fields:    fields,
		Timestamp: n.FirstSeen.UTC().Format(time.RFC3339),
		Footer:    map[string]any{"text": fmt.Sprintf("itemwatch #%d · %s", n.AlertID, n.Rule)},
	}
	if n.Reopened() {
		embed.Description = label("reopened")
	}
	username := d.Username
	if username == "" {
		username = "itemwatch"
	}
	return json.Marshal(map[string]any{
		"username":         username,
		"embeds":           []discordEmbed{embed},
		"allowed_mentions": map[string]any{"parse": []string{}},
	})
}

// Notify posts one notification. 429 and 5xx are retried; other 4xx are not.
func (d *Discord) Notify(ctx context.Context, n queue.Notification) error {
	body, err := d.Payload(n)
	if err != nil {
		return Permanent(err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.URL, bytes.NewReader(body))
	if err != nil {
		return Permanent(err)
	}
	req.Header.Set("Content-Type", "application/json")
	client := d.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	switch {
	case resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return fmt.Errorf("discord: %s", resp.Status)
	default:
		return Permanent(fmt.Errorf("discord: %s: %s", resp.Status, bytes.TrimSpace(snippet)))
	}
}
