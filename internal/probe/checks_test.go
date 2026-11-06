package probe

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const realReply = `<?xml version="1.0"?>` + "\n" +
	`<tsqp version="1.0"><serverinfo uptime="6286" ip="" servername="World" port="7172" location="" url="" server="Crystal" version="1.0" client="15.30"/>` +
	`<owner name="" email=""/><players online="12" unique="10" max="0" peak="3"/><monsters total="0"/><npcs total="0"/>` +
	`<rates experience="1" skill="1" loot="1" magic="1" spawn="1"/><map name="world" author="" width="0" height="0"/><motd>hello</motd></tsqp>`

// statusServer answers one connection with handle.
func statusServer(t *testing.T, handle func(net.Conn)) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				handle(conn)
			}()
		}
	}()
	return l.Addr().String()
}

func reply(t *testing.T, body string) func(net.Conn) {
	return func(conn net.Conn) {
		req := make([]byte, len(statusRequest))
		if _, err := io.ReadFull(conn, req); err != nil {
			t.Errorf("read request: %v", err)
			return
		}
		if string(req) != "\x06\x00\xff\xffinfo" {
			t.Errorf("request % x", req)
		}
		conn.Write([]byte(body))
	}
}

func runStatus(t *testing.T, addr string, timeout time.Duration) (map[string]any, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return OTStatus{Addr: addr}.Run(ctx)
}

func TestOTStatusReadsTheServerReply(t *testing.T) {
	facts, err := runStatus(t, statusServer(t, reply(t, realReply)), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if facts["uptime_seconds"] != int64(6286) || facts["players_online"] != int64(12) {
		t.Fatalf("facts %v", facts)
	}
}

func TestOTStatusDoesNotNeedTheServerToHangUp(t *testing.T) {
	addr := statusServer(t, func(conn net.Conn) {
		reply(t, realReply)(conn)
		time.Sleep(time.Second)
	})
	start := time.Now()
	if _, err := runStatus(t, addr, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Fatalf("waited for the hang up: %s", took)
	}
}

func TestOTStatusAcceptsLatin1Text(t *testing.T) {
	body := strings.Replace(realReply, "hello", "ol\xe1", 1)
	if _, err := runStatus(t, statusServer(t, reply(t, body)), 2*time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestOTStatusRejectsBadReplies(t *testing.T) {
	cases := map[string]string{
		"hang up without reply": "",
		"not xml":               "HTTP/1.1 400 Bad Request\r\n\r\n",
		"other xml":             `<?xml version="1.0"?><html></html>`,
		"truncated":             realReply[:len(realReply)/2],
		"no serverinfo":         `<tsqp version="1.0"><players online="1"/></tsqp>`,
		"no players":            `<tsqp version="1.0"><serverinfo uptime="1"/></tsqp>`,
		"no uptime":             `<tsqp version="1.0"><serverinfo/><players online="1"/></tsqp>`,
		"bad number":            `<tsqp version="1.0"><serverinfo uptime="x"/><players online="1"/></tsqp>`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := runStatus(t, statusServer(t, reply(t, body)), 2*time.Second); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestOTStatusTimesOutOnASilentServer(t *testing.T) {
	addr := statusServer(t, func(conn net.Conn) { io.Copy(io.Discard, conn) })
	start := time.Now()
	if _, err := runStatus(t, addr, 100*time.Millisecond); err == nil {
		t.Fatal("accepted silence")
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("timeout ignored: %s", took)
	}
}

func TestOTStatusRefusesEndlessReply(t *testing.T) {
	addr := statusServer(t, func(conn net.Conn) {
		junk := make([]byte, 8192)
		for {
			if _, err := conn.Write(junk); err != nil {
				return
			}
		}
	})
	if _, err := runStatus(t, addr, 2*time.Second); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("err %v", err)
	}
}

func TestOTStatusClosedPort(t *testing.T) {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := l.Addr().String()
	l.Close()
	if _, err := runStatus(t, addr, time.Second); err == nil {
		t.Fatal("closed port accepted")
	}
}

func runLogin(t *testing.T, handler http.HandlerFunc) error {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := HTTPLogin{URL: srv.URL, Email: "probe@example.com", Password: "secret"}.Run(ctx)
	return err
}

func TestHTTPLoginSendsTheClientFormat(t *testing.T) {
	err := runLogin(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("%s %s", r.Method, r.Header.Get("Content-Type"))
		}
		if body["type"] != "login" || body["email"] != "probe@example.com" || body["password"] != "secret" {
			t.Errorf("body %v", body)
		}
		w.Write([]byte(`{"session":{"sessionkey":"abc"},"playdata":{"worlds":[],"characters":[]}}`))
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestHTTPLoginFailures(t *testing.T) {
	cases := []struct {
		name, want string
		handler    http.HandlerFunc
	}{
		{"refused with 200", "not correct", func(w http.ResponseWriter, _ *http.Request) {
			w.Write([]byte(`{"errorCode":3,"errorMessage":"Account name or password is not correct."}`))
		}},
		{"session and error", "error 3", func(w http.ResponseWriter, _ *http.Request) {
			w.Write([]byte(`{"errorCode":3,"session":{}}`))
		}},
		{"two-factor account", "error 6", func(w http.ResponseWriter, _ *http.Request) {
			w.Write([]byte(`{"errorCode":6,"errorMessage":"Enter the code"}`))
		}},
		{"no session", "no session", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(`{}`)) }},
		{"null session", "no session", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(`{"session":null}`)) }},
		{"server error", "500", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) }},
		{"proxy page", "not JSON", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(`<html>bad gateway</html>`)) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := runLogin(t, c.handler)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err %v, want %q", err, c.want)
			}
		})
	}
}

func TestHTTPLoginDoesNotFollowRedirects(t *testing.T) {
	stolen := false
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { stolen = true }))
	defer other.Close()
	err := runLogin(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL, http.StatusTemporaryRedirect)
	})
	if err == nil || stolen {
		t.Fatalf("err %v, followed %v", err, stolen)
	}
}

func TestHTTPLoginErrorHasNoURL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := HTTPLogin{URL: "http://127.0.0.1:1/login?key=abc", Email: "a", Password: "b"}.Run(ctx)
	if err == nil || strings.Contains(err.Error(), "key=abc") {
		t.Fatalf("err %v", err)
	}
}

type fakeHeartbeat struct {
	err  error
	name string
}

func (f *fakeHeartbeat) Heartbeat(_ context.Context, name string) error {
	f.name = name
	return f.err
}

func TestDBWrite(t *testing.T) {
	db := &fakeHeartbeat{}
	if _, err := (DBWrite{DB: db}).Run(context.Background()); err != nil || db.name != "db-write" {
		t.Fatalf("err %v name %q", err, db.name)
	}
	db.err = errors.New("read only")
	if _, err := (DBWrite{DB: db}).Run(context.Background()); !errors.Is(err, db.err) {
		t.Fatalf("err %v", err)
	}
}
