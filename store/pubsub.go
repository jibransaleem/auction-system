package store

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/redis/go-redis/v9"

	cfg "github.com/jibransaleem/auction-system/engine/configuration"
)

type RedisPublisher struct{ rdb *redis.Client }

func NewRedisPublisher(rdb *redis.Client) *RedisPublisher { return &RedisPublisher{rdb: rdb} }

func (p *RedisPublisher) Publish(ctx context.Context, ev cfg.Event) error {
	if p == nil || p.rdb == nil {
		return errors.New("redis client is nil")
	}
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return p.rdb.Publish(ctx, UpdatesChannel(ev.AuctionID), b).Err()
}