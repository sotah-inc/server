package main

import (
	"flag"
	"fmt"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Misc"
	"github.com/ihsw/go-download/Util"
	"github.com/ihsw/go-download/Work"
	"os"
	"os/signal"
	"runtime"
	"syscall"
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

	/*
		gathering a cache client and regions after reading the config
		gathering the realms for each region
	*/
	var (
		cacheClient Cache.Client
		regions     []Entity.Region
	)
	regionRealms := map[int64][]Entity.Realm{}
	if *flushDb {
		if cacheClient, regions, err = Misc.GetCacheClientAndRegions(*configPath, *flushDb); err != nil {
			output.Write(fmt.Sprintf("Misc.GetCacheClientAndRegions() fail: %s", err.Error()))
			return
		}

		output.Write("Fetching realms for each region...")
		if regionRealms, err = Misc.GetRealms(cacheClient, regions); err != nil {
			output.Write(fmt.Sprintf("Misc.GetRealms() fail: %s", err.Error()))
			return
		}
	} else {
		if cacheClient, _, err = Misc.GetCacheClient(*configPath, *flushDb); err != nil {
			output.Write(fmt.Sprintf("Misc.GetCacheClient() fail: %s", err.Error()))
			return
		}

		regionManager := Entity.RegionManager{Client: cacheClient}
		if regions, err = regionManager.FindAll(); err != nil {
			output.Write(fmt.Sprintf("RegionManager.FindAll() fail: %s", err.Error()))
			return
		}

		realmManager := Entity.RealmManager{Client: cacheClient}
		for _, region := range regions {
			if regionRealms[region.Id], err = realmManager.FindByRegion(region); err != nil {
				output.Write(fmt.Sprintf("RealmManager.FindByRegion() fail: %s", err.Error()))
				return
			}
		}
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
		if !*isProd {
			if len(regionRealms[region.Id]) < 1 {
				output.Write(fmt.Sprintf("Region %s had fewer than 1 realm", region.Name))
				return
			}
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
	if regionRealms, err = queue.DownloadRealms(regionRealms, totalRealms); err != nil {
		output.Write(fmt.Sprintf("Run.WorkQueue() #1 failed (%s)", err.Error()))
		return
	}

	output.Write("Starting up the timed rotation...")
	c := time.Tick(10 * time.Minute)
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGQUIT)
	for {
		select {
		case <-c:

			output.Write("Running it again...")

			if regionRealms, err = queue.DownloadRealms(regionRealms, totalRealms); err != nil {
				output.Write(fmt.Sprintf("Run.WorkQueue() failed (%s)", err.Error()))
				return
			}

			output.Write("Done!")
		case <-sigc:
			output.Write("Halting!")
			output.Conclude()
			return
		}
	}

	output.Conclude()
}
