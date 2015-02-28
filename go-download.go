package main

import (
	"flag"
	"fmt"
	"github.com/ihsw/go-download/Blizzard/Status"
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
	)
	if _, _, regionRealms, err = Misc.Init(*configPath, *flushDb); err != nil {
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

	// queueing it up
	realmsToDo := []Entity.Realm{}
	for _, realms := range formattedRealms {
		for _, realm := range realms {
			realmsToDo = append(realmsToDo, realm)
		}
	}
	out := DownloadRealm.DoWork(realmsToDo, func(realm Entity.Realm) (job DownloadRealm.Job) {
		output.Write(fmt.Sprintf("Working on %s...", realm.Dump()))
		return DownloadRealm.NewJob(realm)
	})

	// waiting for it to drain out
	for job := range out {
		if err = job.Err; err != nil {
			output.Write(fmt.Sprintf("Job failed: %s", err.Error()))
			return
		}
	}

	output.Conclude()
}
