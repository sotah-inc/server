package main

import (
	"errors"
	"fmt"
	"github.com/ihsw/go-download/util"
	"github.com/vmihailenco/redis"
	"os"
)

type logSubscriber struct {
	client *redis.Client
	list   string
}

func (l logSubscriber) Check() (string, error) {
	v := l.client.BLPop(10, l.list)
	if len(v.Val()) == 0 {
		return "", errors.New("Empty response")
	}

	return v.Val()[1], nil
}

func main() {
	util.Write("Starting...")

	if len(os.Args) == 1 {
		util.Write("Expected a list key to blpop from, got nothing")
		return
	}

	list := os.Args[1]
	util.Write(fmt.Sprintf("Blpop'ing from %s...", list))
	l := logSubscriber{
		client: redis.NewTCPClient("127.0.0.1:6379", "", 0),
		list:   list,
	}

	for {
		value, err := l.Check()
		if err != nil {
			continue
		}
		util.Write(value)
	}

	util.Write("Success!")
}
