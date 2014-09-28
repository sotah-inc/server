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
	debug := false

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
		if debug {
			totalRealms += 1
		} else {
			totalRealms += len(regionRealms[region.Id])
		}
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
	itemizeIn := make(chan Work.DownloadResult, totalRealms)
	itemizeOut := make(chan Work.ItemizeResult, totalRealms)

	// spawning some download workers
	downloadWorkerCount := 4
	output.Write(fmt.Sprintf("Spawning %d download workers...", downloadWorkerCount))
	for j := 0; j < downloadWorkerCount; j++ {
		go func(in chan Entity.Realm, out chan Work.DownloadResult, cacheClient Cache.Client) {
			for {
				Work.DownloadRealm(<-in, out, cacheClient)
			}
		}(downloadIn, itemizeIn, cacheClient)
	}

	// spawning an itemize worker
	go func(in chan Work.DownloadResult, out chan Work.ItemizeResult, cacheClient Cache.Client) {
		for {
			Work.ItemizeRealm(<-in, out, cacheClient)
		}
	}(itemizeIn, itemizeOut, cacheClient)

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

	/*
		running the queue
	*/
	// pushing the realms into the start of the queue
	output.Write("Queueing up the realms for checking...")
	for _, realms := range formattedRealms {
		for _, realm := range realms {
			downloadIn <- realm
		}

		if debug {
			break
		}
	}

	// waiting for the itemize results to drain out
	output.Write(fmt.Sprintf("Waiting for %d results to drain out...", totalRealms))
	for i := 0; i < totalRealms; i++ {
		<-itemizeOut
	}

	output.Conclude()
}
