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
	queue := Work.Queue{
		DownloadOut: make(chan Work.DownloadResult, 1),
		ItemizeOut:  make(chan Work.ItemizeResult, 1),
		CacheClient: cacheClient,
	}

	/*
		fetching a region and realm
	*/
	// misc
	regionManager := Entity.RegionManager{Client: cacheClient}
	realmManager := Entity.RealmManager{Client: cacheClient}
	regionName := "us"
	realmSlug := "aegwynn"

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

	// downloading it
	output.Write("Manually running queue.DownloadRealm()...")
	queue.DownloadRealm(realm)
	downloadResult := <-queue.DownloadOut
	if downloadResult.Err != nil {
		output.Write(fmt.Sprintf("downloadOut had an error: %s", downloadResult.Err.Error()))
		return
	}

	if downloadResult.AlreadyChecked {
		output.Write(fmt.Sprintf("Realm %s has already been downloaded", realm.Dump()))
		output.Write(fmt.Sprintf("Realm last-downloaded: %s", realm.LastDownloaded.Format(Util.WriteLayout)))
		output.Write(fmt.Sprintf("Result last-modified: %s", downloadResult.LastModified.Format(Util.WriteLayout)))
		return
	}

	// itemizing it
	output.Write("Manually running queue.ItemizeRealm()...")
	queue.ItemizeRealm(downloadResult)
	itemizeResult := <-queue.ItemizeOut
	if itemizeResult.Err != nil {
		output.Write(fmt.Sprintf("itemizeOut had an error: %s", itemizeResult.Err.Error()))
		return
	}

	output.Conclude()
}
