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

	/*
		reading the config
	*/
	// gathering a cache client after reading the config
	var cacheClient Cache.Client
	cacheClient, _, err = Misc.GetCacheClient(os.Args, false)
	if err != nil {
		output.Write(fmt.Sprintf("Misc.GetCacheClient() fail: %s", err.Error()))
		return
	}

	/*
		bullshit
	*/
	// misc
	totalRealms := 1
	queue := Work.Queue{
		DownloadIn:  make(chan Entity.Realm, totalRealms),
		DownloadOut: make(chan Work.DownloadResult, totalRealms),
		ItemizeOut:  make(chan Work.ItemizeResult, totalRealms),
		CacheClient: cacheClient,
	}

	/*
		fetching a region and realm
	*/
	// misc
	regionManager := Entity.RegionManager{Client: cacheClient}
	realmManager := Entity.RealmManager{Client: cacheClient}
	regionName := "us"
	realmSlug := "earthen-ring"

	// region
	var region Entity.Region
	region, err = regionManager.FindOneByName(regionName)
	if err != nil {
		output.Write(fmt.Sprintf("RealmManager.FindOneByName() fail:  %s", err.Error()))
		return
	}
	if !region.IsValid() {
		output.Write(fmt.Sprintf("Region %s could not be found!", regionName))
	}

	// realm
	var realm Entity.Realm
	realm, err = realmManager.FindOneByRegionAndSlug(region, realmSlug)
	if err != nil {
		output.Write(fmt.Sprintf("RealmManager.FindOneByRegionAndName() fail: %s", err.Error()))
		return
	}
	if !realm.IsValid() {
		output.Write(fmt.Sprintf("Realm %s in region %s could not be found!", realmSlug, regionName))
		return
	}

	/*
		queueing stuff up
	*/
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

	// formatting the realm into a list
	regionRealms := map[int64][]Entity.Realm{}
	regionRealms[region.Id] = append(regionRealms[region.Id], realm)
	if regionRealms, err = queue.DownloadRealms(regionRealms, totalRealms); err != nil {
		output.Write(fmt.Sprintf("Queue.DownloadRealms() fail: %s", err.Error()))
		return
	}

	output.Conclude()
}
