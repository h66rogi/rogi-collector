package pipeline

import (
	"context"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/redis/go-redis/v9"
)

var productChat = redis.NewScript(`
if redis.call('EXISTS',KEYS[1])==0 or redis.call('EXISTS',KEYS[2])==0 then redis.call('SET',KEYS[2],ARGV[1]) end
local id=redis.call('XADD',KEYS[1],'MAXLEN',10000,'*',unpack(ARGV,3))
redis.call('XTRIM',KEYS[1],'MINID',ARGV[2])
redis.call('EXPIRE',KEYS[1],86400)
redis.call('EXPIRE',KEYS[2],86400)
return id
`)

func (p *Publisher) publishProductChat(ctx context.Context, msg model.ChatMessage) error {
	fields := msg.ToStreamFields()
	delete(fields, "raw")
	args := []interface{}{uuid.NewString(), strconv.FormatInt(time.Now().Add(-24*time.Hour).UnixMilli(), 10) + "-0"}
	for k, v := range fields {
		args = append(args, k, v)
	}
	key := chatStreamKey(msg.Platform, msg.ChannelID)
	_, err := productChat.Run(ctx, p.rdb, []string{key, key + ":generation"}, args...).Result()
	if err != nil {
		p.errors.Add(1)
	} else {
		p.published.Add(1)
	}
	return err
}
