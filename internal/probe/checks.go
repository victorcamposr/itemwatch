package probe

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

// statusRequest is the Open Tibia status query: a two-byte little-endian
// length, the status protocol id, and the "info" command.
var statusRequest = []byte{0x06, 0x00, 0xFF, 0xFF, 'i', 'n', 'f', 'o'}

// maxStatusReply bounds what a broken or hostile server can make us buffer.
const maxStatusReply = 64 << 10

var statusEnd = []byte("</tsqp>")

// OTStatus asks the game server for its status over TCP. The answer is built
// by the game itself, so unlike a port check it proves the game still responds.
//
// Servers ignore a repeated query from one address inside their status timeout
// (statusTimeout on Crystal and Canary), so the interval has to be longer than
// that or every other run fails.
type OTStatus struct {
	Addr string
}

// Name implements Check.
func (OTStatus) Name() string { return "ot-status" }

// Run implements Check.
func (c OTStatus) Run(ctx context.Context) (map[string]any, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", c.Addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.SetDeadline(time.Now()) })
	defer stop()

	if _, err := conn.Write(statusRequest); err != nil {
		return nil, fmt.Errorf("send status request: %w", err)
	}
	reply, err := readStatus(conn)
	if err != nil {
		return nil, fmt.Errorf("read status reply: %w", err)
	}
	return parseStatus(reply)
}

// readStatus stops at the closing tag or when the server hangs up, whichever
// comes first: servers differ on whether they close the connection.
func readStatus(r io.Reader) ([]byte, error) {
	var reply bytes.Buffer
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		reply.Write(buf[:n])
		if bytes.Contains(reply.Bytes(), statusEnd) || errors.Is(err, io.EOF) {
			return reply.Bytes(), nil
		}
		if err != nil {
			return nil, err
		}
		if reply.Len() > maxStatusReply {
			return nil, errors.New("reply too large")
		}
	}
}

// parseStatus starts at the root element: that skips any prolog, which may
// name an encoding the decoder refuses. Older servers also send Latin-1 text
// in the free-form fields, which the decoder rejects unless it is cleaned up.
func parseStatus(reply []byte) (map[string]any, error) {
	start := bytes.Index(reply, []byte("<tsqp"))
	if start < 0 {
		return nil, errors.New("reply has no tsqp document")
	}
	var doc struct {
		XMLName    xml.Name `xml:"tsqp"`
		ServerInfo *struct {
			Uptime *int64 `xml:"uptime,attr"`
		} `xml:"serverinfo"`
		Players *struct {
			Online *int64 `xml:"online,attr"`
		} `xml:"players"`
	}
	if err := xml.Unmarshal(bytes.ToValidUTF8(reply[start:], []byte("?")), &doc); err != nil {
		return nil, fmt.Errorf("invalid tsqp document: %w", err)
	}
	if doc.ServerInfo == nil || doc.ServerInfo.Uptime == nil {
		return nil, errors.New("tsqp document has no serverinfo uptime")
	}
	if doc.Players == nil || doc.Players.Online == nil {
		return nil, errors.New("tsqp document has no players online")
	}
	return map[string]any{"uptime_seconds": *doc.ServerInfo.Uptime, "players_online": *doc.Players.Online}, nil
}

// HTTPLogin signs in through the client's HTTP login, in the "my account"
// format. It proves the whole login path: web service, database and account
// data. The login servers answer errors with HTTP 200 and an errorCode, so the
// body decides. Use a dedicated account without two-factor authentication.
type HTTPLogin struct {
	URL      string
	Email    string
	Password string
	Client   *http.Client
}

// Name implements Check.
func (HTTPLogin) Name() string { return "http-login" }

// NewLoginClient refuses redirects, which would resend the password to wherever
// the response points, and keeps no idle connection between runs.
func NewLoginClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableKeepAlives = true
	return &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// Run implements Check.
func (c HTTPLogin) Run(ctx context.Context) (map[string]any, error) {
	body, err := json.Marshal(map[string]any{
		"type": "login", "email": c.Email, "password": c.Password, "stayloggedin": true,
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL, bytes.NewReader(body))
	if err != nil {
		return nil, plainURLError(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	client := c.Client
	if client == nil {
		client = NewLoginClient()
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, plainURLError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("login answered %s", resp.Status)
	}

	var reply struct {
		ErrorCode    json.RawMessage `json:"errorCode"`
		ErrorMessage string          `json:"errorMessage"`
		Session      json.RawMessage `json:"session"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&reply); err != nil {
		return nil, fmt.Errorf("login reply is not JSON: %w", err)
	}
	if code := string(reply.ErrorCode); code != "" && code != "0" && code != "null" {
		return nil, fmt.Errorf("login refused (error %s): %s", code, reply.ErrorMessage)
	}
	if len(reply.Session) == 0 || string(reply.Session) == "null" {
		return nil, errors.New("login reply has no session")
	}
	return nil, nil
}

// plainURLError drops the URL from a client error: this text ends up in the
// alert details and in /v1/status.
func plainURLError(err error) error {
	var u *url.Error
	if errors.As(err, &u) {
		return u.Err
	}
	return err
}

// Heartbeater is the write the db-write check performs.
type Heartbeater interface {
	Heartbeat(ctx context.Context, name string) error
}

// DBWrite proves the state database accepts writes. A database can stay up and
// answer reads while refusing every write, which looks healthy from outside.
type DBWrite struct {
	DB Heartbeater
}

// Name implements Check.
func (DBWrite) Name() string { return "db-write" }

// Run implements Check.
func (c DBWrite) Run(ctx context.Context) (map[string]any, error) {
	return nil, c.DB.Heartbeat(ctx, c.Name())
}
