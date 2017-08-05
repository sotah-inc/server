package main

import (
	"fmt"
	"os"

	"github.com/ihsw/go-download/app/log"
	"github.com/ihsw/go-download/app/util"
)

func main() {
	Util.Write("Starting...")

	if len(os.Args) == 1 {
		Util.Write("Expected a list key to blpop from, got nothing")
		return
	}

	l := Log.New("127.0.0.1:6379", "", 0, os.Args[1])

	Util.Write(fmt.Sprintf("Subscribing to %s...", l.List))
	for {
		value, err := l.Check()
		if err != nil {
			continue
		}
		Util.Write(value)
	}

	Util.Write("Success!")
}
