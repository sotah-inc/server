package Cache

import (
	"fmt"
	"github.com/ihsw/go-download/Config"
	"github.com/vmihailenco/redis"
)

const ITEMS_PER_BUCKET = 1024

// funcs
func NewWrapper(r Config.Redis) (w Wrapper, err error) {
	c := redis.NewTCPClient(r.Host, r.Password, r.Db)
	defer c.Close()

	err = c.Ping().Err()
	if err != nil {
		return
	}

	w = Wrapper{
		Redis: c,
	}
	return
}

func NewClient(redisConfig Config.RedisConfig) (client Client, err error) {
	var w Wrapper

	client.Main, err = NewWrapper(redisConfig.Main)
	if err != nil {
		return
	}

	for _, poolItem := range redisConfig.Pool {
		w, err = NewWrapper(poolItem)
		if err != nil {
			return
		}
		client.Pool = append(client.Pool, w)
	}

	return client, nil
}

func GetBucketKey(id int64, namespace string) (bucketKey string, subKey int64) {
	subKey = id % ITEMS_PER_BUCKET
	bucketId := (id - subKey) / ITEMS_PER_BUCKET
	bucketKey = fmt.Sprintf("%s_bucket:%d", namespace, bucketId)
	return bucketKey, subKey
}

/*
	Wrapper
*/
type Wrapper struct {
	Redis *redis.Client
}

func (self Wrapper) Incr(key string) (int64, error) {
	var v int64
	req := self.Redis.Incr(key)
	if req.Err() != nil {
		return v, req.Err()
	}
	return req.Val(), nil
}

/*
	Client
*/
type Client struct {
	Main Wrapper
	Pool []Wrapper
}

func (self Client) FlushAll() error {
	var (
		req *redis.StatusReq
	)
	req = self.Main.Redis.FlushDb()
	if req.Err() != nil {
		return req.Err()
	}
	for _, w := range self.Pool {
		req = w.Redis.FlushDb()
		if req.Err() != nil {
			return req.Err()
		}
	}

	return nil
}
