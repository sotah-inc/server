package main

import (
	"flag"
	"fmt"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Entity/Character"
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
	var regions []Entity.Region
	if regions, err = regionManager.FindAll(); err != nil {
		output.Write(fmt.Sprintf("RegionManager.FindAll() fail: %s", err.Error()))
		return
	}

	characterCount := 0
	realmManager := Entity.RealmManager{Client: cacheClient}
	var realms []Entity.Realm
	for _, region := range regions {
		if realms, err = realmManager.FindByRegion(region); err != nil {
			output.Write(fmt.Sprintf("RealmManager.FindByRegion() fail: %s", err.Error()))
			return
		}
		for _, realm := range realms {
			characterManager := Character.Manager{Client: cacheClient, Realm: realm}
			var names []string
			if names, err = characterManager.GetNames(); err != nil {
				output.Write(fmt.Sprintf("CharacterManager.GetNames() fail: %s", err.Error()))
				return
			}
			characterCount += len(names)
		}
	}
	output.Write(fmt.Sprintf("Characters in the world: %d", characterCount))

	output.Conclude()
}
