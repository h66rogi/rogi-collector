package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/redis/go-redis/v9"
)

// RedisStore wraps a Redis client for leader election, heartbeats,
// and pub/sub coordination.
type RedisStore struct {
	client *redis.Client
}

// LeaderLeaseResult captures the current state of a leader lease operation.
type LeaderLeaseResult struct {
	Held  bool
	Token int64
}

// NewRedisStore creates a new RedisStore.
func NewRedisStore(client *redis.Client) *RedisStore {
	return &RedisStore{client: client}
}

// StreamLength returns the length of a Redis stream.
func (r *RedisStore) StreamLength(ctx context.Context, key string) (int64, error) {
	return r.client.XLen(ctx, key).Result()
}

// ---------------------------------------------------------------------------
// Leader Election
// ---------------------------------------------------------------------------

// TryAcquireLeader attempts to acquire the leader lock using SET NX PX.
// Returns true if this instance became the leader.
func (r *RedisStore) TryAcquireLeader(ctx context.Context, leaderKey string, instanceID string, ttl time.Duration) (bool, error) {
	result, err := r.TryAcquireLeaderLease(ctx, leaderKey, instanceID, ttl)
	if err != nil {
		return false, err
	}
	return result.Held, nil
}

func leaderFenceCounterKey(leaderKey string) string {
	return fmt.Sprintf("%s:fence:counter", leaderKey)
}

func leaderFenceKey(leaderKey string) string {
	return fmt.Sprintf("%s:fence", leaderKey)
}

// acquireLeaderScript atomically acquires the leader lease and issues a
// monotonic fencing token for the new owner. The leader key value remains the
// raw instance ID for backward compatibility with existing readers.
var acquireLeaderScript = redis.NewScript(`
	local currentToken = redis.call("GET", KEYS[3])
	if redis.call("EXISTS", KEYS[1]) == 0 then
		local token = redis.call("INCR", KEYS[2])
		redis.call("PSETEX", KEYS[1], ARGV[2], ARGV[1])
		redis.call("PSETEX", KEYS[3], ARGV[2], token)
		return {1, token}
	end
	if currentToken then
		return {0, tonumber(currentToken)}
	end
	return {0, 0}
`)

// renewLeaderScript is a Lua script that atomically checks whether the current
// leader matches instanceID and, if so, extends the TTL.
var renewLeaderScript = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		local token = redis.call("GET", KEYS[2])
		if not token then
			token = redis.call("INCR", KEYS[3])
			redis.call("PSETEX", KEYS[2], ARGV[2], token)
		else
			redis.call("PEXPIRE", KEYS[2], ARGV[2])
		end
		redis.call("PEXPIRE", KEYS[1], ARGV[2])
		return {1, tonumber(token)}
	end
	return {0, 0}
`)

func parseLeaderLeaseResult(raw interface{}) (LeaderLeaseResult, error) {
	values, ok := raw.([]interface{})
	if !ok || len(values) != 2 {
		return LeaderLeaseResult{}, fmt.Errorf("unexpected leader lease result: %T", raw)
	}

	held, ok := values[0].(int64)
	if !ok {
		return LeaderLeaseResult{}, fmt.Errorf("unexpected leader lease held value: %T", values[0])
	}

	token, ok := values[1].(int64)
	if !ok {
		return LeaderLeaseResult{}, fmt.Errorf("unexpected leader lease token value: %T", values[1])
	}

	return LeaderLeaseResult{
		Held:  held == 1,
		Token: token,
	}, nil
}

// TryAcquireLeaderLease attempts to acquire the leader lock and returns the
// current fencing token for the active lease.
func (r *RedisStore) TryAcquireLeaderLease(ctx context.Context, leaderKey string, instanceID string, ttl time.Duration) (LeaderLeaseResult, error) {
	raw, err := acquireLeaderScript.Run(ctx, r.client,
		[]string{leaderKey, leaderFenceCounterKey(leaderKey), leaderFenceKey(leaderKey)},
		instanceID,
		ttl.Milliseconds(),
	).Result()
	if err != nil {
		return LeaderLeaseResult{}, fmt.Errorf("try acquire leader: %w", err)
	}

	result, err := parseLeaderLeaseResult(raw)
	if err != nil {
		return LeaderLeaseResult{}, fmt.Errorf("try acquire leader: %w", err)
	}
	return result, nil
}

// RenewLeader extends the leader TTL if this instance is still the leader.
// Returns true if renewal succeeded.
func (r *RedisStore) RenewLeader(ctx context.Context, leaderKey string, instanceID string, ttl time.Duration) (bool, error) {
	result, err := r.RenewLeaderLease(ctx, leaderKey, instanceID, ttl)
	if err != nil {
		return false, err
	}
	return result.Held, nil
}

// RenewLeaderLease extends the leader TTL if this instance is still the leader
// and returns the current fencing token for the active lease.
func (r *RedisStore) RenewLeaderLease(ctx context.Context, leaderKey string, instanceID string, ttl time.Duration) (LeaderLeaseResult, error) {
	raw, err := renewLeaderScript.Run(ctx, r.client,
		[]string{leaderKey, leaderFenceKey(leaderKey), leaderFenceCounterKey(leaderKey)},
		instanceID,
		ttl.Milliseconds(),
	).Result()
	if err != nil {
		return LeaderLeaseResult{}, fmt.Errorf("renew leader: %w", err)
	}

	result, err := parseLeaderLeaseResult(raw)
	if err != nil {
		return LeaderLeaseResult{}, fmt.Errorf("renew leader: %w", err)
	}
	return result, nil
}

// ValidateLeaderLease checks whether the given fencing token is still the
// active leader lease.  Returns true only when the stored token matches.
func (r *RedisStore) ValidateLeaderLease(ctx context.Context, leaderKey string, token int64) (bool, error) {
	val, err := r.client.Get(ctx, leaderFenceKey(leaderKey)).Int64()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("validate leader lease: %w", err)
	}
	return val == token, nil
}

// ReleaseLeaderLease atomically deletes the leader lease keys only if the
// current holder matches instanceID.  Used for graceful shutdown so the
// standby can acquire immediately without waiting for TTL expiry.
var releaseLeaderScript = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		redis.call("DEL", KEYS[1])
		redis.call("DEL", KEYS[2])
		return 1
	end
	return 0
`)

func (r *RedisStore) ReleaseLeaderLease(ctx context.Context, leaderKey string, instanceID string) (bool, error) {
	result, err := releaseLeaderScript.Run(ctx, r.client,
		[]string{leaderKey, leaderFenceKey(leaderKey)},
		instanceID,
	).Int64()
	if err != nil {
		return false, fmt.Errorf("release leader lease: %w", err)
	}
	return result == 1, nil
}

// GetLeader returns the current leader instance ID, or empty string if none.
func (r *RedisStore) GetLeader(ctx context.Context, leaderKey string) (string, error) {
	val, err := r.client.Get(ctx, leaderKey).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("get leader: %w", err)
	}
	return val, nil
}

// ---------------------------------------------------------------------------
// Worker Heartbeat
// ---------------------------------------------------------------------------

const heartbeatTTL = 15 * time.Second

func heartbeatKey(workerID string) string {
	return fmt.Sprintf("worker:heartbeat:%s", workerID)
}

// SendHeartbeat stores the worker's load data with a 30-second TTL.
func (r *RedisStore) SendHeartbeat(ctx context.Context, workerID string, load model.WorkerLoad) error {
	data, err := json.Marshal(load)
	if err != nil {
		return fmt.Errorf("marshal heartbeat: %w", err)
	}
	return r.client.Set(ctx, heartbeatKey(workerID), data, heartbeatTTL).Err()
}

// GetWorkerHeartbeat returns the latest heartbeat for a worker, or nil if
// the key has expired or does not exist.
func (r *RedisStore) GetWorkerHeartbeat(ctx context.Context, workerID string) (*model.WorkerLoad, error) {
	val, err := r.client.Get(ctx, heartbeatKey(workerID)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get heartbeat: %w", err)
	}

	var load model.WorkerLoad
	if err := json.Unmarshal([]byte(val), &load); err != nil {
		return nil, fmt.Errorf("unmarshal heartbeat: %w", err)
	}
	return &load, nil
}

// GetWorkerHeartbeats returns the latest heartbeats for multiple workers in a
// single MGET call. Workers without a heartbeat are omitted from the result map.
func (r *RedisStore) GetWorkerHeartbeats(ctx context.Context, workerIDs []string) (map[string]*model.WorkerLoad, error) {
	if len(workerIDs) == 0 {
		return make(map[string]*model.WorkerLoad), nil
	}

	keys := make([]string, len(workerIDs))
	for i, id := range workerIDs {
		keys[i] = heartbeatKey(id)
	}

	vals, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("mget heartbeats: %w", err)
	}

	result := make(map[string]*model.WorkerLoad, len(workerIDs))
	for i, v := range vals {
		if v == nil {
			continue
		}
		s, ok := v.(string)
		if !ok {
			continue
		}
		var load model.WorkerLoad
		if err := json.Unmarshal([]byte(s), &load); err != nil {
			continue
		}
		result[workerIDs[i]] = &load
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// Coordinator Events (Pub/Sub)
// ---------------------------------------------------------------------------

// CoordEventType enumerates coordinator event types.
type CoordEventType string

const (
	CoordEventAssign       CoordEventType = "assign"
	CoordEventUnassign     CoordEventType = "unassign"
	CoordEventEnd          CoordEventType = "end"
	CoordEventWSClosed     CoordEventType = "ws_closed"
	CoordEventDrainStarted CoordEventType = "drain_started"
)

// CoordEvent is published by the coordinator to notify about channel assignments.
type CoordEvent struct {
	Type      CoordEventType `json:"type"`
	Platform  model.Platform `json:"platform"`
	ChannelID string         `json:"channelId"`
	WorkerID  string         `json:"workerId"`
}

const coordEventsChannel = "coord:events"

// PublishCoordEvent publishes a coordinator event to all subscribers.
func (r *RedisStore) PublishCoordEvent(ctx context.Context, event CoordEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal coord event: %w", err)
	}
	return r.client.Publish(ctx, coordEventsChannel, data).Err()
}

// SubscribeCoordEvents returns a pub/sub subscription for coordinator events.
func (r *RedisStore) SubscribeCoordEvents(ctx context.Context) *redis.PubSub {
	return r.client.Subscribe(ctx, coordEventsChannel)
}

// ---------------------------------------------------------------------------
// Worker Commands (Pub/Sub)
// ---------------------------------------------------------------------------

// WorkerCommandType enumerates worker command types.
type WorkerCommandType string

const (
	WorkerCommandConnect    WorkerCommandType = "connect"
	WorkerCommandDisconnect WorkerCommandType = "disconnect"
	// WorkerCommandReconnect tears down the existing chat connection (if any) and
	// re-establishes it with fresh chatChannelId/access-token. Use to recover
	// silently stuck channels (e.g., chzzk session rotated mid-broadcast).
	WorkerCommandReconnect WorkerCommandType = "reconnect"
)

// WorkerCommand is sent to a specific worker to connect/disconnect a channel.
type WorkerCommand struct {
	Type      WorkerCommandType `json:"type"`
	Platform  model.Platform    `json:"platform"`
	ChannelID string            `json:"channelId"`
}

func workerCommandChannel(workerID string) string {
	return fmt.Sprintf("worker:cmd:%s", workerID)
}

// PublishWorkerCommand sends a command to a specific worker.
func (r *RedisStore) PublishWorkerCommand(ctx context.Context, workerID string, cmd WorkerCommand) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal worker command: %w", err)
	}
	return r.client.Publish(ctx, workerCommandChannel(workerID), data).Err()
}

// SubscribeWorkerCommands returns a pub/sub subscription for a worker's command channel.
func (r *RedisStore) SubscribeWorkerCommands(ctx context.Context, workerID string) *redis.PubSub {
	return r.client.Subscribe(ctx, workerCommandChannel(workerID))
}

// ---------------------------------------------------------------------------
// Discover Commands (Pub/Sub)
// ---------------------------------------------------------------------------

const discoverCommandChannel = "discover:cmd"

// DiscoverCommandType enumerates discover-side admin commands.
type DiscoverCommandType string

const (
	// DiscoverCommandRediscover requests an immediate discovery cycle. The
	// Platform field, when non-empty, scopes the trigger; otherwise all
	// platforms are triggered. ChannelID is passed for logging only — the
	// underlying TriggerDiscovery is platform-wide, not single-channel.
	DiscoverCommandRediscover DiscoverCommandType = "rediscover"
)

// DiscoverCommand is a leader-processed admin signal sent to discover instances.
type DiscoverCommand struct {
	Type      DiscoverCommandType `json:"type"`
	Platform  string              `json:"platform,omitempty"`
	ChannelID string              `json:"channelId,omitempty"`
}

// PublishDiscoverCommand sends an admin command to all discover instances; the
// leader instance acts on it and the rest ignore.
func (r *RedisStore) PublishDiscoverCommand(ctx context.Context, cmd DiscoverCommand) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal discover command: %w", err)
	}
	return r.client.Publish(ctx, discoverCommandChannel, data).Err()
}

// SubscribeDiscoverCommands returns a pub/sub subscription for discover admin commands.
func (r *RedisStore) SubscribeDiscoverCommands(ctx context.Context) *redis.PubSub {
	return r.client.Subscribe(ctx, discoverCommandChannel)
}

// ReadStreamMessages reads recent messages from a per-channel Redis Stream
// using XREVRANGE. Returns messages in reverse chronological order.
func (r *RedisStore) ReadStreamMessages(ctx context.Context, platform, channelID string, count int, beforeID string) ([]StreamMessage, error) {
	if count <= 0 || count > 200 {
		count = 50
	}
	streamKey := fmt.Sprintf("chat:%s:%s", platform, channelID)
	end := "+"
	if beforeID != "" {
		end = "(" + beforeID // exclusive
	}
	msgs, err := r.client.XRevRangeN(ctx, streamKey, end, "-", int64(count)).Result()
	if err != nil {
		return nil, fmt.Errorf("xrevrange %s: %w", streamKey, err)
	}

	result := make([]StreamMessage, 0, len(msgs))
	for _, msg := range msgs {
		result = append(result, StreamMessage{
			StreamID: msg.ID,
			Values:   msg.Values,
		})
	}
	return result, nil
}

// StreamMessage represents a single Redis Stream entry.
type StreamMessage struct {
	StreamID string
	Values   map[string]interface{}
}

// ReadFirehoseMessages reads recent messages from the chat:firehose Redis Stream
// using XREVRANGE. Returns messages in reverse chronological order.
func (r *RedisStore) ReadFirehoseMessages(ctx context.Context, count int, beforeID string) ([]StreamMessage, error) {
	if count <= 0 || count > 200 {
		count = 50
	}
	streamKey := "chat:firehose"
	end := "+"
	if beforeID != "" {
		end = "(" + beforeID // exclusive
	}
	msgs, err := r.client.XRevRangeN(ctx, streamKey, end, "-", int64(count)).Result()
	if err != nil {
		return nil, fmt.Errorf("xrevrange %s: %w", streamKey, err)
	}

	result := make([]StreamMessage, 0, len(msgs))
	for _, msg := range msgs {
		result = append(result, StreamMessage{
			StreamID: msg.ID,
			Values:   msg.Values,
		})
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// Stream Cleanup
// ---------------------------------------------------------------------------

// ChatStreamKey returns the Redis key for a per-channel chat stream.
func ChatStreamKey(platform, channelID string) string {
	return fmt.Sprintf("chat:%s:%s", platform, channelID)
}

// DeleteChatStreams asynchronously unlinks per-channel chat streams in batches.
// Uses UNLINK instead of DEL to avoid blocking the Redis server on large streams.
// Returns the number of keys actually deleted.
func (r *RedisStore) DeleteChatStreams(ctx context.Context, keys []string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	var deleted int64
	const batchSize = 100
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		n, err := r.client.Unlink(ctx, keys[i:end]...).Result()
		if err != nil {
			return deleted, fmt.Errorf("unlink chat streams batch: %w", err)
		}
		deleted += n
	}
	return deleted, nil
}

// ScanChatStreamKeys filters by Redis type so generation metadata is never treated as an orphan stream.
// Returns all matching keys. Suitable for periodic cleanup jobs.
func (r *RedisStore) ScanChatStreamKeys(ctx context.Context) ([]string, error) {
	var keys []string
	var cursor uint64
	for {
		batch, nextCursor, err := r.client.ScanType(ctx, cursor, "chat:*:*", 500, "stream").Result()
		if err != nil {
			return keys, fmt.Errorf("scan chat stream keys: %w", err)
		}
		keys = append(keys, batch...)
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	return keys, nil
}

// NewRedisClient creates a new Redis client from a connection address.
func NewRedisClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: os.Getenv("REDIS_PASSWORD"),
	})
}
