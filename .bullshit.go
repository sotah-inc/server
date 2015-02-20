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
	realmManager := Entity.RealmManager{Client: cacheClient, RegionManager: regionManager}
	var regions []Entity.Region
	if regions, err = regionManager.FindAll(); err != nil {
		output.Write(fmt.Sprintf("RegionManager.FindAll() fail: %s", err.Error()))
		return
	}
	realmCount := 0
	for _, region := range regions {
		var realms []Entity.Realm
		if realms, err = realmManager.FindByRegion(region); err != nil {
			output.Write(fmt.Sprintf("RealmManager.FindByRegion() fail: %s", err.Error()))
			return
		}
		realmCount += len(realms)

		for _, realm := range realms {
			if !realm.Region.IsValid() {
				output.Write(fmt.Sprintf("Realm %d region is invalid!", realm.Id))
				return
			}
		}
	}
	output.Write(fmt.Sprintf("Realm count: %d", realmCount))

	output.Conclude()
}
