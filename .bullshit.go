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
		DownloadOut: make(chan Work.DownloadResult, 1),
	}
	queue.DownloadRealm(realm, false)
	downloadResult := <-queue.DownloadOut
	if err = downloadResult.Err; err != nil {
		output.Write(fmt.Sprintf("downloadResult had an error: %s", err.Error()))
		return
	}
	if downloadResult.AlreadyChecked {
		output.Write(fmt.Sprintf("Realm %s was already checked!", realm.Dump()))
		return
	}
	if downloadResult.ResponseFailed {
		output.Write(fmt.Sprintf("Realm %s response failed!", realm.Dump()))
		return
	}
	output.Write(fmt.Sprintf("Realm %s has %d auctions", realm.Dump(), len(downloadResult.AuctionDataResponse.Auctions.Auctions)))

	output.Conclude()
}
