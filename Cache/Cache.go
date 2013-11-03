package Cache

import (
	"encoding/json"
	"fmt"
	"github.com/ihsw/go-download/Config"
	"github.com/vmihailenco/redis/v2"
	"strconv"
)

const ITEMS_PER_BUCKET = 1024

// funcs
func NewWrapper(r Config.Redis) (w Wrapper, err error) {
	c := redis.NewTCPClient(&redis.Options{
		Addr:     r.Host,
		Password: r.Password,
		DB:       r.Db,
	})
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
type Manager interface {
	Namespace() string
}

type Wrapper struct {
	Redis *redis.Client
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
	// misc
	var values []map[string]interface{}

	// forwarding to the FetchFromIds method
	ids := make([]int64, 1)
	ids[0] = id
	values, err = self.FetchFromIds(manager, ids)
	if err != nil {
		return
	}

	return values[0], nil
}

func (self Wrapper) FetchFromIds(manager Manager, ids []int64) (values []map[string]interface{}, err error) {
	// misc
	idsLength := len(ids)

	// optionally halting on empty ids list
	if idsLength == 0 {
		return values, err
	}

	// gathering input from the ids
	redis := self.Redis
	results := make([]string, idsLength)
	for i, id := range ids {
		bucketKey, subKey := GetBucketKey(id, manager.Namespace())
		cmd := redis.HGet(bucketKey, subKey)
		if cmd.Err() != nil {
			return values, cmd.Err()
		}
		results[i] = cmd.Val()
	}

	// json-decoding them all
	decodedResults := make([]map[string]interface{}, idsLength)
	for i, result := range results {
		b := []byte(result)
		err = json.Unmarshal(b, &decodedResults[i])
		if err != nil {
			return values, err
		}
	}

	return decodedResults, err
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
		cmd *redis.StatusCmd
	)
	cmd = self.Main.Redis.FlushDb()
	if cmd.Err() != nil {
		return cmd.Err()
	}
	for _, w := range self.Pool {
		cmd = w.Redis.FlushDb()
		if cmd.Err() != nil {
			return cmd.Err()
		}
	}

	return nil
}
