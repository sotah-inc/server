package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/ihsw/go-download/Blizzard/Auction"
	"github.com/ihsw/go-download/Blizzard/AuctionData"
	"github.com/ihsw/go-download/Blizzard/Status"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Misc"
	"github.com/ihsw/go-download/Queue/DownloadRealm"
	"github.com/ihsw/go-download/Util"
	"runtime"
	"time"
)

type StatusGetResult struct {
	region   Entity.Region
	response Status.Response
	err      error
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flushDb := flag.Bool("flush", false, "Clears all redis dbs")
	configPath := flag.String("config", "", "Config path")
	// isProd := flag.Bool("prod", false, "Prod mode")
	flag.Parse()

	output := Util.Output{StartTime: time.Now()}
	output.Write("Starting...")

	// init
	var (
		regionRealms map[int64][]Entity.Realm
		err          error
		cacheClient  Cache.Client
	)
	if cacheClient, _, regionRealms, err = Misc.Init(*configPath, *flushDb); err != nil {
		output.Write(fmt.Sprintf("Misc.Init() fail: %s", err.Error()))
		return
	}

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

	// misc
	realmsToDo := make(chan Entity.Realm)
	downloadJobs := DownloadRealm.DoWork(realmsToDo, func(realm Entity.Realm) (job DownloadRealm.Job) {
		// misc
		realmManager := Entity.NewRealmManager(realm.Region, cacheClient)
		job = DownloadRealm.NewJob(realm)

		// fetching the auction info
		var (
			auctionResponse *Auction.Response
			err             error
		)
		if auctionResponse, err = Auction.Get(realm, cacheClient.ApiKey); err != nil {
			job.Err = errors.New(fmt.Sprintf("Auction.Get() failed (%s)", err.Error()))
			return
		}

		// optionally halting on empty response
		if auctionResponse == nil {
			job.ResponseFailed = true
			return
		}

		file := auctionResponse.Files[0]

		// checking whether the file has already been downloaded
		lastModified := time.Unix(file.LastModified/1000, 0)
		if !realm.LastDownloaded.IsZero() && (realm.LastDownloaded.Equal(lastModified) || realm.LastDownloaded.After(lastModified)) {
			job.AlreadyChecked = true
			return
		}

		// fetching the actual auction data
		if job.AuctionDataResponse = AuctionData.Get(realm, file.Url); job.AuctionDataResponse == nil {
			job.ResponseFailed = true
			return
		}

		// dumping the auction data for parsing after itemize-results are tabulated
		if err = job.DumpData(); err != nil {
			job.Err = errors.New(fmt.Sprintf("DownloadResult.dumpData() failed (%s)", err.Error()))
			return
		}

		// flagging the realm as having been downloaded
		realm.LastDownloaded = lastModified
		realm.LastChecked = time.Now()
		if realm, err = realmManager.Persist(realm); err != nil {
			job.Err = errors.New(fmt.Sprintf("RealmManager.Persist() failed (%s)", err.Error()))
			return
		}

		return
	})

	// starting it up
	go func() {
		for _, realms := range formattedRealms {
			for _, realm := range realms {
				realmsToDo <- realm
			}
		}
		close(realmsToDo)
	}()

	// waiting for it to drain out
	for job := range downloadJobs {
		if err = job.Err; err != nil {
			output.Write(fmt.Sprintf("Job for realm %s failed: %s", job.Realm.Dump(), err.Error()))
			continue
		}
	}

	output.Conclude()
}
