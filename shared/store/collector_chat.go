package store

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// The generation and retained range are read atomically with the messages.
var chatRead = redis.NewScript(`
local generation=redis.call('GET',KEYS[2]) or ''
local earliest=redis.call('XRANGE',KEYS[1],'-','+','COUNT',1)
local latest=redis.call('XREVRANGE',KEYS[1],'+','-','COUNT',1)
local rows=redis.call('XRANGE',KEYS[1],'('..ARGV[1],'+','COUNT',100)
return {generation,earliest,latest,rows}
`)

type ChatBatch struct {
	Generation, Earliest, Latest string
	Messages                     []StreamMessage
}

func (s *RedisStore) ReadProductChat(ctx context.Context, channel, after string) (ChatBatch, error) {
	var batch ChatBatch
	if !ValidStreamID(after) {
		return batch, errors.New("invalid stream cursor")
	}
	key := ChatStreamKey("soop", channel)
	result, err := chatRead.Run(ctx, s.client, []string{key, key + ":generation"}, after).Slice()
	if err != nil {
		return batch, err
	}
	batch.Generation, _ = result[0].(string)
	for index, target := range map[int]*string{1: &batch.Earliest, 2: &batch.Latest} {
		if rows, ok := result[index].([]interface{}); ok && len(rows) > 0 {
			row := rows[0].([]interface{})
			*target = row[0].(string)
		}
	}
	if rows, ok := result[3].([]interface{}); ok {
		for _, raw := range rows {
			row := raw.([]interface{})
			fields := row[1].([]interface{})
			msg := StreamMessage{StreamID: row[0].(string), Values: map[string]interface{}{}}
			for i := 0; i+1 < len(fields); i += 2 {
				msg.Values[fields[i].(string)] = fields[i+1]
			}
			batch.Messages = append(batch.Messages, msg)
		}
	}
	return batch, nil
}
func ValidStreamID(id string) bool {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return false
	}
	for _, p := range parts {
		if p == "" {
			return false
		}
		for _, r := range p {
			if r < '0' || r > '9' {
				return false
			}
		}
		if _, e := strconv.ParseUint(p, 10, 64); e != nil {
			return false
		}
	}
	return true
}
func StreamIDBefore(a, b string) bool {
	aa := strings.Split(a, "-")
	bb := strings.Split(b, "-")
	if len(aa) != 2 || len(bb) != 2 {
		return false
	}
	for i := 0; i < 2; i++ {
		av, _ := strconv.ParseUint(aa[i], 10, 64)
		bv, _ := strconv.ParseUint(bb[i], 10, 64)
		if av != bv {
			return av < bv
		}
	}
	return false
}
func (s *RedisStore) NotifyDonations(ctx context.Context, channel string) error {
	return s.client.Publish(ctx, "collector:donations:"+channel, "changed").Err()
}
func (s *RedisStore) TrimProductChat(ctx context.Context, channel string) error {
	return s.client.XTrimMinID(ctx, ChatStreamKey("soop", channel), strconv.FormatInt(time.Now().Add(-24*time.Hour).UnixMilli(), 10)+"-0").Err()
}
