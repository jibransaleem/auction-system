package store

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	cfg "github.com/jibransaleem/auction-system/engine/configuration"
)

// Writes only if the new version is higher than the stored one.
var saveScript = redis.NewScript(`
local cur = tonumber(redis.call('HGET', KEYS[1], 'version') or '-1')
if cur >= tonumber(ARGV[1]) then return 0 end
redis.call('HSET', KEYS[1],
  'version', ARGV[1], 'price', ARGV[2], 'stock_left', ARGV[3],
  'ends_at', ARGV[4], 'status', ARGV[5])
return 1
`)

type RedisStore struct {
	rdb *redis.Client
}

func NewRedisStore(rdb *redis.Client) *RedisStore {
	return &RedisStore{rdb: rdb}
}

func stateKey(id string) string { return "auction:" + id + ":state" }

func (s *RedisStore) SaveState(ctx context.Context, st cfg.AuctionState) error {
	return saveScript.Run(ctx, s.rdb,
		[]string{stateKey(st.AuctionID)},
		st.Version,
		st.Price,
		st.StockLeft,
		st.EndAt.UnixMilli(),
		string(st.Status),
	).Err()
}

// LoadState returns a zero AuctionState (no error) if the key does not exist.
func (s *RedisStore) LoadState(ctx context.Context, id string) (cfg.AuctionState, error) {
	m, err := s.rdb.HGetAll(ctx, stateKey(id)).Result()
	if err != nil || len(m) == 0 {
		return cfg.AuctionState{}, err
	}
	ver, _ := strconv.ParseUint(m["version"], 10, 64)
	price, _ := strconv.ParseInt(m["price"], 10, 64)
	stock, _ := strconv.Atoi(m["stock_left"])
	ends, _ := strconv.ParseInt(m["ends_at"], 10, 64)

	return cfg.AuctionState{
		AuctionID: id,
		Version:   ver,
		Price:     price,
		StockLeft: stock,
		EndAt:     time.UnixMilli(ends),
		Status:    cfg.AuctionStatus(m["status"]),
	}, nil
}