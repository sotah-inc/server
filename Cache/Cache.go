package Cache

import (
	"encoding/json"
	"fmt"
	"github.com/ihsw/go-download/Config"
	"github.com/vmihailenco/redis"
	"strconv"
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

func GetBucketKey(id int64, namespace string) (string, string) {
	remainder := id % ITEMS_PER_BUCKET
	bucketId := (id - remainder) / ITEMS_PER_BUCKET

	bucketKey := fmt.Sprintf("%s_bucket:%d", namespace, bucketId)
	subKey := strconv.FormatInt(remainder, 10)
	return bucketKey, subKey
}

/*
	Wrapper
*/
type Wrapper struct {
	Redis *redis.Client
}

type Manager interface {
	Namespace() string
}

func (self Wrapper) FetchIds(key string, start int64, end int64) (ids []int64, err error) {
	req := self.Redis.LRange(key, start, end)
	if req.Err() != nil {
		return ids, req.Err()
	}

	// optionally halting
	length := len(req.Val())
	if length == 0 {
		return ids, nil
	}

	// converting them
	ids = make([]int64, length)
	var i int
	for k, v := range req.Val() {
		i, err = strconv.Atoi(v)
		if err != nil {
			return ids, err
		}
		ids[k] = int64(i)
	}

	return ids, nil
}

func (self Wrapper) FetchFromId(manager Manager, id int64) (v map[string]interface{}, err error) {
	var s string
	bucketKey, subKey := GetBucketKey(id, manager.Namespace())
	req := self.Redis.HGet(bucketKey, subKey)
	if req.Err() != nil {
		return v, req.Err()
	}

	b := []byte(s)
	err = json.Unmarshal(b, &v)
	if err != nil {
		return
	}

	return
}

func (self Wrapper) FetchFromIds(manager Manager, ids []int64) ([]map[string]interface{}, error) {
	// misc
	var (
		// s        string
		// v        map[string]interface{}
		err      error
		pipeline *redis.PipelineClient
		list     []map[string]interface{}
	)
	pipeline, err = self.Redis.PipelineClient()
	if err != nil {
		return list, err
	}

	// optionally halting
	length := len(ids)
	if length == 0 {
		return list, err
	}
	list = make([]map[string]interface{}, length)

	// going over the ids
	for _, id := range ids {
		bucketKey, subKey := GetBucketKey(id, manager.Namespace())
		pipeline.HGet(bucketKey, subKey)
	}

	return list, err
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
