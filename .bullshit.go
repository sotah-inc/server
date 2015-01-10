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
	// gathering a cache client and regions after reading the config
	output.Write("Initializing the cache-client...")
	var (
		cacheClient Cache.Client
	)
	cacheClient, _, err = Misc.GetCacheClient(os.Args, false)
	if err != nil {
		output.Write(fmt.Sprintf("Misc.GetCacheClient() fail: %s", err.Error()))
		return
	}

	/*
		bullshit
	*/
	realmManager := Entity.RealmManager{Client: cacheClient}
	downloadIn := make(chan Entity.Realm, 1)
	downloadOut := make(chan Work.DownloadResult, 1)
	itemizeOut := make(chan Work.ItemizeResult, 1)

	// fetching a realm
	var realm Entity.Realm
	realm, err = realmManager.FindOneById(1)
	if err != nil {
		output.Write(fmt.Sprintf("RealmManager.FindOneById() fail: %s", err.Error()))
		return
	}

	queue := Work.Queue{
		DownloadIn:  downloadIn,
		DownloadOut: downloadOut,
		ItemizeOut:  itemizeOut,
		CacheClient: cacheClient,
	}

	output.Write("Manually running queue.DownloadRealm()...")
	queue.DownloadRealm(realm)
	downloadResult := <-queue.DownloadOut
	if downloadResult.Err != nil {
		output.Write(fmt.Sprintf("downloadOut had an error: %s", downloadResult.Err.Error()))
		return
	}

	output.Write("Manually running queue.ItemizeRealm()...")
	queue.ItemizeRealm(downloadResult)
	itemizeResult := <-queue.ItemizeOut
	if itemizeResult.Err != nil {
		output.Write(fmt.Sprintf("itemizeOut had an error: %s", itemizeResult.Err.Error()))
		return
	}

	output.Conclude()
}
