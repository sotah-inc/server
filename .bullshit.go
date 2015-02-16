package main

import (
	"flag"
	"fmt"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Misc"
	"github.com/ihsw/go-download/Util"
	"runtime"
	"time"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	configPath := flag.String("config", "", "Config path")
	flag.Parse()

	output := Util.Output{StartTime: time.Now()}
	output.Write("Starting...")

	var err error

	/*
		reading the config
	*/
	// gathering a cache client after reading the config
	var cacheClient Cache.Client
	cacheClient, _, err = Misc.GetCacheClient(*configPath, false)
	if err != nil {
		output.Write(fmt.Sprintf("Misc.GetCacheClient() fail: %s", err.Error()))
		return
	}

	/*
		bullshit
	*/
	regionManager := Entity.RegionManager{Client: cacheClient}
	var regions []Entity.Region
	if regions, err = regionManager.FindAll(); err != nil {
		output.Write(fmt.Sprintf("RegionManager.FindAll() fail: %s", err.Error()))
		return
	}

	for _, region := range regions {
		region.Queryable = true
		if region, err = regionManager.Persist(region); err != nil {
			output.Write(fmt.Sprintf("RegionManager.Persist() fail: %s", err.Error()))
			return
		}
	}

	output.Conclude()
}
