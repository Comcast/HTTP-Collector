package config

import (
	"flag"
	"os"
	"time"

	"github.com/go-redis/redis/v7"
)

const (
	DefaultTTL = 25 * time.Hour
	MaxTTL     = 24 * 31 * time.Hour
)

var redisPwd = flag.String("redispwd", os.Getenv("REDIS_PASSWORD"), "The Redis password if any, env: REDIS_PASSWORD")

type Redis struct {
	ClientConfig *redis.UniversalOptions
	Keyspace     string
	TTL          int `json:"ttl"`
	ttl          time.Duration
}

func NewRedis() *Redis {
	return &Redis{
		ClientConfig: &redis.UniversalOptions{},
		Keyspace:     "quota", // default keyspace to use
		TTL:          86400,   // unit in second, default 1 day TTL
	}
}

func (r *Redis) GetTTL() time.Duration {
	return r.ttl
}

func (r *Redis) Validate() error {
	if redisPwd != nil && *redisPwd != "" {
		r.ClientConfig.Password = *redisPwd
	}

	r.ttl = time.Duration(r.TTL) * time.Second
	if r.ttl == 0 || r.ttl > MaxTTL {
		r.ttl = DefaultTTL
	}
	return nil
}
