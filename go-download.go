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
	output.Write("Initializing the cache-client and regions...")
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

	regionMap := map[int64]int64{}
	for i, region := range regions {
		regionMap[region.Id] = int64(i)
	}

	/*
		making channels and spawning workers
	*/
	// misc
	downloadIn := make(chan Entity.Realm, totalRealms)
	downloadOut := make(chan Work.DownloadResult, totalRealms)
	itemizeOut := make(chan Work.ItemizeResult, totalRealms)

	// spawning some download and itemize workers
	downloadWorkerCount := 4
	output.Write(fmt.Sprintf("Spawning %d download and itemize workers...", downloadWorkerCount))
	for j := 0; j < downloadWorkerCount; j++ {
		go func(in chan Entity.Realm, cacheClient Cache.Client, out chan Work.DownloadResult) {
			for {
				Work.DownloadRealm(<-in, cacheClient, out)
			}
		}(downloadIn, cacheClient, downloadOut)
	}

	go func(in chan Work.DownloadResult, cacheClient Cache.Client, out chan Work.ItemizeResult) {
		for {
			Work.ItemizeRealm(<-in, cacheClient, out)
		}
	}(downloadOut, cacheClient, itemizeOut)

	/*
		going over the list
	*/
	output.Write("Running it once to start it up...")
	regionRealms, err = Work.RunQueue(regionRealms, downloadIn, itemizeOut, totalRealms, cacheClient, false)
	if err != nil {
		output.Write(fmt.Sprintf("Run.WorkQueue() #1 failed (%s)", err.Error()))
		return
	}

	output.Write("Starting up the timed rotation...")
	c := time.Tick(5 * time.Second)
	for {
		<-c

		output.Write("Running it again...")

		// err = Work.RunQueue(regionRealms, downloadIn, itemizeOut, totalRealms, cacheClient)
		regionRealms, err = Work.RunQueue(regionRealms, downloadIn, itemizeOut, totalRealms, cacheClient, true)
		if err != nil {
			output.Write(fmt.Sprintf("Run.WorkQueue() failed (%s)", err.Error()))
			return
		}
	}

	output.Conclude()
}
