package main

import (
	"flag"
	"fmt"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Misc"
	"github.com/ihsw/go-download/Util"
	"runtime"
	"time"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	configPath := flag.String("config", "", "Config path")
	flushDb := flag.Bool("flush", false, "Clears all redis dbs")
	flag.Parse()

	output := Util.Output{StartTime: time.Now()}
	output.Write("Starting...")

	// init
	var (
		err         error
		cacheClient Cache.Client
	)
	if cacheClient, _, _, err = Misc.Init(*configPath, *flushDb); err != nil {
		output.Write(fmt.Sprintf("Misc.Init() fail: %s", err.Error()))
		return
	}

	regionManager := Entity.NewRegionManager(cacheClient)
	var region Entity.Region
	if region, err = regionManager.FindOneByName("us"); err != nil {
		output.Write(fmt.Sprintf("RegionManager.FindOneByName() fail: %s", err.Error()))
		return
	}

	if !region.IsValid() {
		output.Write(fmt.Sprintf("Region us coult not be found!"))
		return
	}

	realmManager := Entity.NewRealmManager(region, cacheClient)
	var realm Entity.Realm
	if realm, err = realmManager.FindOneBySlug("earthen-ring"); err != nil {
		output.Write(fmt.Sprintf("RealmManager.FindOneBySlug() fail: %s", err.Error()))
		return
	}

	realm.LastDownloaded = time.Now()
	if realm, err = realmManager.Persist(realm); err != nil {
		output.Write(fmt.Sprintf("RealmManager.Persist() fail: %s", err.Error()))
		return
	}

	output.Write(fmt.Sprintf("Realm: %s", realm.Dump()))

	output.Conclude()
}
