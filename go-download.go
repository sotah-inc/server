package main

import (
	"errors"
	"flag"
	"fmt"
	"runtime"
	"time"

	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Misc"
	"github.com/ihsw/go-download/Queue/DownloadRealm"
	"github.com/ihsw/go-download/Queue/ItemizeRealm"
	"github.com/ihsw/go-download/Util"
)

func work(inRealms []map[int64]Entity.Realm, cacheClient Cache.Client) (formattedRealms []map[int64]Entity.Realm, err error) {
	// misc
	formattedRealms = inRealms
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
		// optionally skipping where there's already an error, or the job was skipped over
		if err != nil || !job.CanContinue() {
			continue
		}

		// optionally checking for an error
		if err = job.Err; err != nil {
			continue
		}
	}

	// waiting for alternate-out-done to clear
	<-itemizeAlternateOutDone

	err = errors.New("YOU AREN'T UPDATING REFORMATTED REALMS WITH THE NEW JOB REALM")

	return
}

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

	if !*isProd {
		output.Write("Running it once due to non-prod mode...")
		_, err = work(formattedRealms, cacheClient)
		if err != nil {
			output.Write(fmt.Sprintf("work() fail: %s", err.Error()))
			return
		}
	} else {
		output.Write("Starting up the timed rotation...")
		c := time.Tick(15 * time.Minute)
		for {
			output.Write("Running work()...")
			startTime := time.Now()
			formattedRealms, err = work(formattedRealms, cacheClient)
			if err != nil {
				output.Write(fmt.Sprintf("work() fail: %s", err.Error()))
				return
			}
			output.Write(fmt.Sprintf("Done work() in %.2fs!", time.Since(startTime).Seconds()))

			<-c
		}
	}

	output.Conclude()
}
