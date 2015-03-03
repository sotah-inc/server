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

	// init
	var (
		err         error
		cacheClient Cache.Client
	)
	if cacheClient, _, _, err = Misc.Init(*configPath, false); err != nil {
		output.Write(fmt.Sprintf("Misc.Init() fail: %s", err.Error()))
		return
	}

	regionManager := Entity.NewRegionManager(cacheClient)
	var regions []Entity.Region
	if regions, err = regionManager.FindAll(); err != nil {
		output.Write(fmt.Sprintf("RegionManager.FindAll() fail: %s", err.Error()))
		return
	}

	characterCount := 0
	for _, region := range regions {
		realmManager := Entity.NewRealmManager(region, cacheClient)
		var realms []Entity.Realm
		if realms, err = realmManager.FindAll(); err != nil {
			output.Write(fmt.Sprintf("RealmManager.FindAll() fail: %s", err.Error()))
			return
		}

		for _, realm := range realms {
			characterManager := Character.NewManager(realm, cacheClient)
			var names []string
			if names, err = characterManager.GetNames(); err != nil {
				output.Write(fmt.Sprintf("CharacterManager.GetNames() fail: %s", err.Error()))
				return
			}
			characterCount += len(names)
		}
	}
	output.Write(fmt.Sprintf("Characters in the world: %s", characterCount))

	output.Conclude()
}
