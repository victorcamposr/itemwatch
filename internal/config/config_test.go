package config

import (
	"strings"
	"testing"
	"time"
)

func required(t *testing.T) {
	t.Helper()
	t.Setenv("ITEMWATCH_GAME_DSN", "u:p@tcp(db:3306)/game")
	t.Setenv("ITEMWATCH_STATE_DSN", "u:p@tcp(db:3306)/state")
}

func TestProbesAreOffByDefault(t *testing.T) {
	required(t)
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.ProbeStatusAddr != "" || c.ProbeLoginURL != "" || c.ProbeDBWrite {
		t.Fatalf("probes on without configuration: %+v", c)
	}
	if c.ProbeInterval != 30*time.Second || c.ProbeFailAfter != 5*time.Minute {
		t.Fatalf("defaults %s %s", c.ProbeInterval, c.ProbeFailAfter)
	}
}

func TestProbeSettings(t *testing.T) {
	required(t)
	t.Setenv("ITEMWATCH_PROBE_STATUS_ADDR", "game:7171")
	t.Setenv("ITEMWATCH_PROBE_LOGIN_URL", "https://example.com/login")
	t.Setenv("ITEMWATCH_PROBE_LOGIN_EMAIL", "probe@example.com")
	t.Setenv("ITEMWATCH_PROBE_LOGIN_PASSWORD", "secret")
	t.Setenv("ITEMWATCH_PROBE_DB_WRITE", "true")
	t.Setenv("ITEMWATCH_PROBE_INTERVAL", "1m")
	t.Setenv("ITEMWATCH_PROBE_FAIL_AFTER", "10m")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !c.ProbeDBWrite || c.ProbeInterval != time.Minute || c.ProbeFailAfter != 10*time.Minute {
		t.Fatalf("%+v", c)
	}
}

func TestBadProbeSettings(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"status without port", map[string]string{"PROBE_STATUS_ADDR": "game"}, "host:port"},
		{"login without password", map[string]string{
			"PROBE_LOGIN_URL": "https://example.com/login", "PROBE_LOGIN_EMAIL": "a@b.c",
		}, "go together"},
		{"password alone", map[string]string{"PROBE_LOGIN_PASSWORD": "x"}, "go together"},
		{"login url without scheme", map[string]string{
			"PROBE_LOGIN_URL": "example.com/login", "PROBE_LOGIN_EMAIL": "a@b.c", "PROBE_LOGIN_PASSWORD": "x",
		}, "http or https"},
		{"bad interval", map[string]string{"PROBE_INTERVAL": "soon"}, "PROBE_INTERVAL"},
		{"bad db write", map[string]string{"PROBE_DB_WRITE": "maybe"}, "PROBE_DB_WRITE"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			required(t)
			for k, v := range c.env {
				t.Setenv("ITEMWATCH_"+k, v)
			}
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err %v, want %q", err, c.want)
			}
		})
	}
}
