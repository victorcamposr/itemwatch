package queue

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestDepthWithRedisDown(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	defer rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	pending, dead := Depth(ctx, rdb, "s", "g", "d")
	if pending != -1 || dead != -1 {
		t.Fatalf("pending %d, dead %d", pending, dead)
	}
}

func group(name string, pending, lag int) string {
	return fmt.Sprintf("*12\r\n$4\r\nname\r\n$%d\r\n%s\r\n$9\r\nconsumers\r\n:1\r\n$7\r\npending\r\n:%d\r\n"+
		"$17\r\nlast-delivered-id\r\n$3\r\n0-0\r\n$12\r\nentries-read\r\n:0\r\n$3\r\nlag\r\n:%d\r\n",
		len(name), name, pending, lag)
}

func TestDepth(t *testing.T) {
	cases := []struct {
		name, xinfo string
		pending     int64
	}{
		{"read and unread entries both count", "*2\r\n" + group("other", 9, 9) + group("g", 2, 3), 5},
		{"unknown lag", "*1\r\n" + strings.Replace(group("g", 2, 0), ":0\r\n", "$-1\r\n", 2), 2},
		{"no consumer group", "*0\r\n", 0},
		{"stream never created", "-ERR no such key\r\n", 0},
		{"broken stream", "-WRONGTYPE not a stream\r\n", -1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			addr := fakeRedis(t, map[string]string{"XINFO": c.xinfo, "XLEN": ":4\r\n"})
			rdb := redis.NewClient(&redis.Options{Addr: addr, Protocol: 2, MaxRetries: -1})
			defer rdb.Close()
			pending, dead := Depth(context.Background(), rdb, "s", "g", "d")
			if pending != c.pending || dead != 4 {
				t.Fatalf("pending %d, dead %d", pending, dead)
			}
		})
	}
}

// fakeRedis answers each command from a table of raw RESP replies, enough for
// the client's handshake to fall back and for the commands under test.
func fakeRedis(t *testing.T, replies map[string]string) string {
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
				in := bufio.NewReader(conn)
				for {
					command, err := readCommand(in)
					if err != nil {
						return
					}
					reply, ok := replies[strings.ToUpper(command)]
					if !ok {
						reply = "-ERR unknown command\r\n"
					}
					conn.Write([]byte(reply))
				}
			}()
		}
	}()
	return l.Addr().String()
}

// readCommand reads one array of bulk strings and returns its first element.
func readCommand(in *bufio.Reader) (string, error) {
	line, err := in.ReadString('\n')
	if err != nil {
		return "", err
	}
	count, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "*")))
	if err != nil {
		return "", err
	}
	var first string
	for i := range count {
		if _, err := in.ReadString('\n'); err != nil { // the $length line
			return "", err
		}
		value, err := in.ReadString('\n')
		if err != nil {
			return "", err
		}
		if i == 0 {
			first = strings.TrimSpace(value)
		}
	}
	return first, nil
}
