package main

import (
	"flag"
	"fmt"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Misc"
	"github.com/ihsw/go-download/Util"
	"github.com/ihsw/go-download/Work"
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
	if cacheClient, _, err = Misc.GetCacheClient(*configPath, false); err != nil {
		output.Write(fmt.Sprintf("Misc.GetCacheClient() fail: %s", err.Error()))
		return
	}

	/*
		bullshit
	*/
	regionManager := Entity.RegionManager{Client: cacheClient}
	realmManager := Entity.RealmManager{Client: cacheClient}
	var region Entity.Region
	if region, err = regionManager.FindOneByName("us"); err != nil {
		output.Write(fmt.Sprintf("RegionManager.FindOneByName() fail: %s", err.Error()))
		return
	}
	var realm Entity.Realm
	if realm, err = realmManager.FindOneByRegionAndSlug(region, "earthen-ring"); err != nil {
		output.Write(fmt.Sprintf("RealmManager.FindOneByRegionAndSlug() fail: %s", err.Error()))
		return
	}

	queue := Work.Queue{
		CacheClient: cacheClient,
		DownloadIn:  make(chan Entity.Realm, 1),
		DownloadOut: make(chan Work.DownloadResult, 1),
		ItemizeOut:  make(chan Work.ItemizeResult, 1),
	}

	go func(queue Work.Queue) {
		for {
			queue.DownloadRealm(<-queue.DownloadIn, false)
		}
	}(queue)
	go func(queue Work.Queue) {
		for {
			queue.ItemizeRealm(<-queue.DownloadOut)
		}
	}(queue)

	regionRealms := map[int64][]Entity.Realm{}
	regionRealms[region.Id] = []Entity.Realm{realm}
	if regionRealms, err = queue.DownloadRealms(regionRealms, 1); err != nil {
		output.Write(fmt.Sprintf("queue.DownloadRealms() fail: %s", err.Error()))
		return
	}

	output.Conclude()
}
