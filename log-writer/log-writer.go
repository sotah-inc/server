package main

import (
	"fmt"
	"github.com/ihsw/go-download/log"
	"github.com/ihsw/go-download/util"
	"os"
)

func main() {
	util.Write("Starting...")

	if len(os.Args) == 1 {
		util.Write("Expected a list key to blpop from, got nothing")
		return
	}

	l := log.New("127.0.0.1:6379", "", 0, os.Args[1])

	util.Write(fmt.Sprintf("Subscribing to %s...", l.List))
	for {
		value, err := l.Check()
		if err != nil {
			continue
		}
		util.Write(value)
	}

	util.Write("Success!")
}
