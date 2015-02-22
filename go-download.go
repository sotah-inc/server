package main

import (
	"flag"
	"fmt"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Config"
	"github.com/ihsw/go-download/Util"
	"runtime"
	"time"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flushDb := flag.Bool("flush", false, "Clears all redis dbs")
	configPath := flag.String("config", "", "Config path")
	isProd := flag.Bool("prod", false, "Prod mode")
	flag.Parse()

	output := Util.Output{StartTime: time.Now()}
	output.Write("Starting...")

	var err error

	// opening the config file
	var configFile Config.File
	if configFile, err = Config.New(*configPath); err != nil {
		output.Write(fmt.Sprintf("Config.New() fail: %s", err.Error()))
		return
	}

	// connecting the client
	var client Cache.Client
	if client, err = Cache.NewClient(configFile); err != nil {
		output.Write(fmt.Sprintf("Cache.NewClient() fail: %s", err.Error()))
		return
	}
	if *flushDb {
		if err = client.FlushDb(); err != nil {
			output.Write(fmt.Sprintf("CacheClient.FlushDb() fail: %s", err.Error()))
			return
		}
	}

	output.Conclude()
}
