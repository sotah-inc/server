package main

import (
	"flag"
	"fmt"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Misc"
	"github.com/ihsw/go-download/Queue/DownloadRealm"
	"github.com/ihsw/go-download/Queue/ItemizeRealm"
	"github.com/ihsw/go-download/Util"
	"runtime"
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

			if !*isProd {
				break
			}
		}
	}

	// misc
	realmsToDo := make(chan Entity.Realm)
	downloadJobs := DownloadRealm.DoWork(realmsToDo, cacheClient)
	itemizeJobs, itemizeAlternateOutDone := ItemizeRealm.DoWork(downloadJobs, cacheClient)

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
	for job := range itemizeJobs {
		// misc
		realm := job.Realm

		// optionally halting on error
		if err = job.Err; err != nil {
			output.Write(fmt.Sprintf("Job for realm %s failed: %s", realm.Dump(), err.Error()))
			continue
		}

		if !job.CanContinue() {
			if job.AlreadyChecked {
				output.Write(fmt.Sprintf("Realm %s was already checked", realm.Dump()))
			} else if job.ResponseFailed {
				output.Write(fmt.Sprintf("Realm %s fetching response failed", realm.Dump()))
			}

			continue
		}

		output.Write(fmt.Sprintf("Job %s successfully completed", realm.Dump()))
	}

	// waiting for alternate-out-done to clear
	output.Write("Waiting for itemize-alt-out to clear")
	<-itemizeAlternateOutDone

	output.Conclude()
}
