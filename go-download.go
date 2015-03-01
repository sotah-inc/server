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
	itemizeJobs := ItemizeRealm.DoWork(downloadJobs, cacheClient)

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
		if err = job.Err; err != nil {
			output.Write(fmt.Sprintf("Job for realm %s failed: %s", job.Realm.Dump(), err.Error()))
			continue
		}

		output.Write(fmt.Sprintf("Job %s successfully completed", job.Realm.Dump()))
	}

	output.Conclude()
}
