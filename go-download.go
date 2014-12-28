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

	output := Util.Output{
		StartTime: time.Now(),
	}
	output.Write("Starting...")

	var (
		err          error
		cacheClient  Cache.Client
		regions      []Entity.Region
		regionRealms map[int64][]Entity.Realm
	)
	debug := true

	/*
		reading the config
	*/
	// gathering a cache client and regions after reading the config
	output.Write("Initializing the cache-client and regions...")
	cacheClient, regions, err = Misc.GetCacheClientAndRegions(os.Args, true)
	if err != nil {
		output.Write(fmt.Sprintf("Misc.GetCacheClientAndRegions() fail: %s", err.Error()))
		return
	}

	/*
		gathering the realms for each region
	*/
	output.Write("Fetching realms for each region...")
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

		go func(in chan Work.DownloadResult, cacheClient Cache.Client, out chan Work.ItemizeResult) {
			for {
				Work.ItemizeRealm(<-in, cacheClient, out)
			}
		}(downloadOut, cacheClient, itemizeOut)
	}

	/*
		preparation for queueing
	*/
	// formatting the realms to be evenly distributed
	largestRegion := 0
	for _, realms := range regionRealms {
		if len(realms) > largestRegion {
			largestRegion = len(realms)
		}
	}
	formattedRealms := make([]map[int64]Entity.Realm, largestRegion)
	for regionId, realms := range regionRealms {
		for i, realm := range realms {
			if formattedRealms[int64(i)] == nil {
				formattedRealms[int64(i)] = map[int64]Entity.Realm{}
			}
			formattedRealms[int64(i)][regionId] = realm
		}
	}

	// c := time.Tick(5 * time.Minute)
	output.Write("Starting it up...")
	err = Work.RunQueue(formattedRealms, downloadIn, itemizeOut, totalRealms, cacheClient)
	if err != nil {
		output.Write(fmt.Sprintf("Run.WorkQueue fail: %s", err.Error()))
		return
	}
	// for now := range c {
	// Work.RunQueue(formattedRealms, downloadIn, itemizeOut, totalRealms, cacheClient)
	// }

	output.Conclude()
}
