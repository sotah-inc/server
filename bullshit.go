package main

import (
	"fmt"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Entity/Character"
	"github.com/ihsw/go-download/Misc"
	"github.com/ihsw/go-download/Util"
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
	output.Write("Initializing the cache-client and regions...")
	var (
		cacheClient Cache.Client
	)
	cacheClient, err = Misc.GetCacheClient(os.Args, false)
	if err != nil {
		output.Write(fmt.Sprintf("Misc.GetCacheClient() fail: %s", err.Error()))
		return
	}

	regionManager := Entity.RegionManager{Client: cacheClient}
	var regions []Entity.Region
	regions, err = regionManager.FindAll()
	if err != nil {
		output.Write(fmt.Sprintf("RegionManager.FindAll() fail: %s", err.Error()))
		return
	}

	realmManager := Entity.RealmManager{Client: cacheClient}
	characterManager := Character.Manager{Client: cacheClient}
	var (
		realms     []Entity.Realm
		characters []Character.Character
	)

	output.Write("Starting up the timed rotation...")
	c := time.Tick(5 * time.Second)
	for {
		output.Write("Running the timed rotation...")

		characterCount := 0
		for _, region := range regions {
			realms, err = realmManager.FindByRegion(region)
			if err != nil {
				output.Write(fmt.Sprintf("RealmManager.FindByRegion() fail: %s", err.Error()))
				return
			}

			for _, realm := range realms {
				characters, err = characterManager.FindByRealm(realm)
				if err != nil {
					output.Write(fmt.Sprintf("CharacterManager.FindByRealm() fail: %s", err.Error()))
					return
				}

				characterCount += len(characters)
			}
		}
		output.Write(fmt.Sprintf("There are %d characters in the world", characterCount))

		<-c
	}

	output.Conclude()
}
