package Log

import (
	"errors"
	"github.com/vmihailenco/redis"
)

type Log struct {
	client *redis.Client
	List   string
}

func New(host string, password string, db int64, list string) Log {
	return Log{
		client: redis.NewTCPClient(host, password, db),
		List:   list,
	}
}

func (this Log) Check() (string, error) {
	v := this.client.BLPop(10, this.List)
	if len(v.Val()) == 0 {
		return "", errors.New("Empty response")
	}

	return v.Val()[1], nil
}

func (this Log) Write(message string) {
	this.client.RPush(this.List, message)
}
