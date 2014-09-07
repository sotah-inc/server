package main

import (
	"errors"
	"fmt"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Config"
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
		client       Cache.Client
		configFile   Config.ConfigFile
		regions      []Entity.Region
		regionRealms map[int64][]Entity.Realm
	)

	/*
		reading the config
	*/
	// getting a client
	output.Write("Initializing the config...")
	args := os.Args
	if len(args) == 1 {
		err = errors.New("Expected path to config file, got nothing")
		return
	}

	// loading the config-file
	configFile, err = Config.NewConfigFile(args[1])
	if err != nil {
		return
	}

	// connecting the redis clients
	client, err = Misc.NewCacheClient(configFile.ConnectionList)
	if err != nil {
		return
	}

	// flushing all of the databases
	err = client.FlushDb()
	if err != nil {
		return
	}

	// loading the regions and locales
	output.Write("Loading the regions and locales...")
	regions, err = Misc.GetRegions(client, configFile.Regions)
	if err != nil {
		output.Write(fmt.Sprintf("load() fail: %s", err.Error()))
		return
	}

	/*
		gathering and persisting realms for each region
	*/
	output.Write("Fetching realms for each region...")
	regionRealms, err = Misc.GetRealms(client, regions)
	if err != nil {
		output.Write(fmt.Sprintf("getRealms() fail: %s", err.Error()))
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
		// totalRealms += len(regionRealms[region.Id])
		totalRealms += 1
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
	output.Write("Spawning some download workers...")
	downloadWorkerCount := 4
	for j := 0; j < downloadWorkerCount; j++ {
		go func(in chan Entity.Realm, out chan Work.DownloadResult, output Util.Output) {
			for {
				Work.DownloadRealm(<-in, out, output)
			}
		}(downloadIn, itemizeIn, output)
	}

	// spawning an itemize worker
	go func(in chan Work.DownloadResult, out chan Work.ItemizeResult) {
		for {
			Work.ItemizeRealm(<-in, out)
		}
	}(itemizeIn, itemizeOut)

	/*
		queueing up the realms
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

	// pushing the realms into the start of the queue
	output.Write("Queueing up the realms for checking...")
	for _, realms := range formattedRealms {
		for _, realm := range realms {
			downloadIn <- realm
		}
		break
	}

	/*
		debugging
	*/
	output.Write(fmt.Sprintf("Gathering %d results for debugging...", totalRealms))
	results := make([]Work.ItemizeResult, totalRealms)
	for i := 0; i < totalRealms; i++ {
		results[i] = <-itemizeOut
	}

	output.Write(fmt.Sprintf("Going over %d results for debugging...", len(results)))
	totalAuctionCount := 0
	for _, result := range results {
		realm := result.Realm
		if result.Error != nil {
			output.Write(fmt.Sprintf("Itemize %s fail: %s", realm.Dump(), result.Error.Error()))
			continue
		}

		totalAuctionCount += result.AuctionCount
		output.Write(fmt.Sprintf("%s has %d auctions...", realm.Dump(), result.AuctionCount))
	}
	output.Write(fmt.Sprintf("%d auctions in the world...", totalAuctionCount))

	output.Conclude()
}
