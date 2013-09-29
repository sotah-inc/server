package Cache

import (
	"github.com/ihsw/go-download/Config"
	"github.com/vmihailenco/redis"
)

type Cache struct {
	Main *redis.Client
	Pool []*redis.Client
}

func Connect(redisConfig Config.RedisConfig) (Cache, error) {
	var cache Cache
	var err error

	cache.Main, err = Config.GetRedis(redisConfig.Main)
	if err != nil {
		return cache, err
	}

	for _, poolItem := range redisConfig.Pool {
		c, err := Config.GetRedis(poolItem)
		if err != nil {
			return cache, err
		}
		cache.Pool = append(cache.Pool, c)
	}

	return cache, nil
}

func Incr(key string, c *redis.Client) (int64, error) {
	var v int64
	req := c.Incr(key)
	if req.Err() != nil {
		return v, req.Err()
	}
	return req.Val(), nil
}
