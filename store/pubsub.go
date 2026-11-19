package store

import (
	"context"
	"encoding/json"

	"github.com/redis/go-redis/v9"

	cfg "github.com/jibransaleem/auction-system/engine/configuration"
)

type RedisPublisher struct{ rdb *redis.Client }

func NewRedisPublisher(rdb *redis.Client) *RedisPublisher { return &RedisPublisher{rdb: rdb} }

func UpdatesChannel(id string) string { return "auction:" + id + ":updates" }

func (p *RedisPublisher) Publish(ctx context.Context, ev cfg.Event) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return p.rdb.Publish(ctx, UpdatesChannel(ev.AuctionID), b).Err()
}