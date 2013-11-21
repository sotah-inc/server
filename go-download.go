package main

import (
	"errors"
	"fmt"
	"github.com/ihsw/go-download/Blizzard/Status"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Config"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Util"
	"os"
)

func initialize(args []string) (configFile Config.ConfigFile, client Cache.Client, err error) {
	if len(args) == 1 {
		err = errors.New("Expected path to config file, got nothing")
		return
	}

	// loading the config-file
	configFile, err = Config.New(args[1])
	if err != nil {
		return
	}

	// connecting the redis clients
	client, err = Config.NewCacheClient(configFile.ConnectionList)
	if err != nil {
		return
	}

	// flushing all of the databases
	err = client.FlushDb()
	if err != nil {
		return
	}

	return
}

func load(configRegions []Config.Region) (regions []Entity.Region) {

}

func main() {
	Util.Write("Starting...")

	var (
		err        error
		client     Cache.Client
		configFile Config.ConfigFile
	)

	/*
		initialization
	*/
	configFile, client, err = initialize(os.Args)
	if err != nil {
		Util.Write(fmt.Sprintf("initialize() fail: %s", err.Error()))
		return
	}

	/*
		reading the config
	*/
	// managers
	regionManager := Entity.RegionManager{Client: client}
	localeManager := Entity.LocaleManager{Client: client}
	realmManager := Entity.RealmManager{Client: client}

	// persisting the regions and locales
	Util.Write(fmt.Sprintf("Persisting %d regions...", len(configFile.Regions)))
	regions := map[int64]Entity.Region{}
	for _, configRegion := range configFile.Regions {
		region := configRegion.ToEntity()
		region, err = regionManager.Persist(region)
		if err != nil {
			Util.Write(fmt.Sprintf("regionManager.Persist() fail: %s", err.Error()))
			return
		}

		Util.Write(fmt.Sprintf("Persisting %d locales belonging to %s...", len(configRegion.Locales), region.Name))
		for _, configLocale := range configRegion.Locales {
			locale := configLocale.ToEntity()
			locale.Region = region
			locale, err = localeManager.Persist(locale)
			if err != nil {
				Util.Write(fmt.Sprintf("localeManager.Persist() fail: ", err.Error()))
				return
			}
		}
		regions[region.Id] = region
	}

	// going over the regions to download the statuses
	Util.Write(fmt.Sprintf("Going over %d regions to download the statuses...", len(regions)))
	c := make(chan Status.Result, len(regions))
	for _, region := range regions {
		go Status.Get(region, c)
	}

	// gathering the results
	Util.Write(fmt.Sprintf("Gathering the results for %d regions...", len(regions)))
	results := make([]Status.Result, len(regions))
	for i := 0; i < len(results); i++ {
		results[i] = <-c
	}

	// going over the results
	Util.Write(fmt.Sprintf("Going over %d results...", len(results)))
	realmCount := 0
	for _, result := range results {
		if err = result.Error; err != nil {
			Util.Write(fmt.Sprintf("Status.Get() fail: %s", err.Error()))
			return
		}

		region := result.Region
		statusRealms := result.Status.Realms
		Util.Write(fmt.Sprintf("Persisting %d realms belonging to %s...", len(statusRealms), region.Name))
		for _, statusRealm := range statusRealms {
			realm := statusRealm.ToEntity()
			realm.Region = region

			realm, err = realmManager.Persist(realm)
			if err != nil {
				Util.Write(fmt.Sprintf("realmManager.Persist() fail: %s", err.Error()))
				return
			}

			realmCount++
		}
	}
	Util.Write(fmt.Sprintf("Persisted %d realms!", realmCount))

	Util.Conclude()
}
