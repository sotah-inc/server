package Cache

import (
	"github.com/ihsw/go-download/Config"
	"github.com/vmihailenco/redis"
)

// funcs
func Connect(redisConfig Config.RedisConfig) (Client, error) {
	var (
		err    error
		client Client
	)

	client.Main, err = Config.GetRedis(redisConfig.Main)
	if err != nil {
		return client, err
	}

	for _, poolItem := range redisConfig.Pool {
		c, err := Config.GetRedis(poolItem)
		if err != nil {
			return client, err
		}
		client.Pool = append(client.Pool, c)
	}

	return client, nil
}

func Incr(key string, c *redis.Client) (int64, error) {
	var v int64
	req := c.Incr(key)
	if req.Err() != nil {
		return v, req.Err()
	}
	return req.Val(), nil
}

// structs
type Client struct {
	Main *redis.Client
	Pool []*redis.Client
}

func (self Client) FlushAll() error {
	var (
		req *redis.StatusReq
	)
	req = self.Main.FlushDb()
	if req.Err() != nil {
		return req.Err()
	}
	for _, c := range self.Pool {
		req = c.FlushDb()
		if req.Err() != nil {
			return req.Err()
		}
	}

	return nil
}
