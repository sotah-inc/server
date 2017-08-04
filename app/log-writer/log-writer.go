package main

import (
	"fmt"
	"github.com/ihsw/go-download/Log"
	"github.com/ihsw/go-download/Util"
	"os"
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
