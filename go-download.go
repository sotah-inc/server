package main

import (
	"fmt"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Misc"
	"github.com/ihsw/go-download/Util"
	"github.com/ihsw/go-download/Work"
	"os"
	"runtime"
	"time"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	output := Util.Output{StartTime: time.Now()}
	output.Write("Starting...")

	var err error
	debug := true

	/*
		reading the config
	*/
	// gathering a cache client and regions after reading the config
	var (
		cacheClient Cache.Client
		regions     []Entity.Region
	)
	cacheClient, regions, err = Misc.GetCacheClientAndRegions(os.Args, true)
	if err != nil {
		output.Write(fmt.Sprintf("Misc.GetCacheClientAndRegions() fail: %s", err.Error()))
		return
	}

	/*
		gathering the realms for each region
	*/
	output.Write("Fetching realms for each region...")
	var regionRealms map[int64][]Entity.Realm
	regionRealms, err = Misc.GetRealms(cacheClient, regions)
	if err != nil {
		output.Write(fmt.Sprintf("Misc.GetRealms() fail: %s", err.Error()))
		return
	}

	/*
		removing realms that aren't queryable
	*/
	totalRealms := 0
	for _, region := range regions {
		if !region.Queryable {
			delete(regionRealms, region.Id)
			continue
		}

		// optionally truncating the lists of realms
		if debug {
			regionRealms[region.Id] = regionRealms[region.Id][:1]
		}

		totalRealms += len(regionRealms[region.Id])
	}

	/*
		making channels and spawning workers
	*/
	// misc
	queue := Work.Queue{
		DownloadIn:  make(chan Entity.Realm, totalRealms),
		DownloadOut: make(chan Work.DownloadResult, totalRealms),
		ItemizeOut:  make(chan Work.ItemizeResult, totalRealms),
		CacheClient: cacheClient,
	}

	// spawning some download and itemize workers
	downloadWorkerCount := 4
	for j := 0; j < downloadWorkerCount; j++ {
		go func(queue Work.Queue) {
			for {
				queue.DownloadRealm(<-queue.DownloadIn)
			}
		}(queue)
	}
	go func(queue Work.Queue) {
		for {
			queue.ItemizeRealm(<-queue.DownloadOut)
		}
	}(queue)

	/*
		going over the list
	*/
	output.Write("Running it once to start it up...")
	regionRealms, err = queue.DownloadRealms(regionRealms, totalRealms, false)
	if err != nil {
		output.Write(fmt.Sprintf("Run.WorkQueue() #1 failed (%s)", err.Error()))
		return
	}

	output.Conclude()
}
