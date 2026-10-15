// Package queue carries alert notifications over a Redis Stream.
//
// The alert row in MariaDB is the source of truth; the stream only moves
// "this alert needs to be announced" to the dispatchers. Publishing happens
// after the database commit, so a crash in between leaves the alert marked
// unpublished and it is published again on the next pass. Consumers are
// therefore idempotent (see the dispatch package).
package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Notification is the stream payload. It is self-contained so a dispatcher
// never needs the database to deliver it.
type Notification struct {
	AlertID     int64     `json:"alert_id"`
	Generation  int       `json:"generation"`
	Rule        string    `json:"rule"`
	Severity    int8      `json:"severity"`
	Title       string    `json:"title"`
	PlayerName  string    `json:"player_name,omitempty"`
	Origin      string    `json:"origin,omitempty"`
	ItemHash    string    `json:"item_hash,omitempty"`
	Occurrences int64     `json:"occurrences"`
	FirstSeen   time.Time `json:"first_seen"`
}

// Reopened tells whether this is a resolved incident happening again.
func (n Notification) Reopened() bool { return n.Generation > 0 }

// IdempotencyKey identifies one delivery of one alert generation on one channel.
func (n Notification) IdempotencyKey(prefix, channel string) string {
	return fmt.Sprintf("%s:delivered:%s:%d:%d", prefix, channel, n.AlertID, n.Generation)
}

const payloadField = "payload"

// Encode builds the stream entry values.
func Encode(n Notification) (map[string]any, error) {
	b, err := json.Marshal(n)
	if err != nil {
		return nil, err
	}
	return map[string]any{payloadField: string(b)}, nil
}

// Decode reads a stream entry.
func Decode(msg redis.XMessage) (Notification, error) {
	var n Notification
	raw, ok := msg.Values[payloadField].(string)
	if !ok {
		return n, fmt.Errorf("message %s has no payload", msg.ID)
	}
	if err := json.Unmarshal([]byte(raw), &n); err != nil {
		return n, fmt.Errorf("message %s: %w", msg.ID, err)
	}
	return n, nil
}

// Publisher appends notifications to the stream.
type Publisher struct {
	rdb    *redis.Client
	stream string
	maxLen int64
}

// NewPublisher returns a Publisher that keeps roughly maxLen entries.
func NewPublisher(rdb *redis.Client, stream string, maxLen int64) *Publisher {
	return &Publisher{rdb: rdb, stream: stream, maxLen: maxLen}
}

// Publish appends one notification and returns the entry id.
func (p *Publisher) Publish(ctx context.Context, n Notification) (string, error) {
	values, err := Encode(n)
	if err != nil {
		return "", err
	}
	return p.rdb.XAdd(ctx, &redis.XAddArgs{Stream: p.stream, MaxLen: p.maxLen, Approx: true, Values: values}).Result()
}
