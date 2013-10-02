package Cache

import (
	"github.com/vmihailenco/redis"
)

// interfaces
type Redis interface {
	Host() string
	Password() string
	Db() int64
}

type RedisConfig interface {
	Main() Redis
	Pool() []Redis
}

// funcs
func GetRedis(r Redis) (*redis.Client, error) {
	c := redis.NewTCPClient(r.Host(), r.Password(), r.Db())
	defer c.Close()
	return c, c.Ping().Err()
}

func NewClient(redisConfig RedisConfig) (client Client, err error) {
	var c *redis.Client

	client.Main, err = GetRedis(redisConfig.Main())
	if err != nil {
		return
	}

	for _, poolItem := range redisConfig.Pool() {
		c, err = GetRedis(poolItem)
		if err != nil {
			return
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

/*
	Client
*/
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
