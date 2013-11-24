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

func initialize(args []string) (Config.ConfigFile, Cache.Client, error) {
	var (
		configFile Config.ConfigFile
		client     Cache.Client
		err        error
	)

	if len(args) == 1 {
		err = errors.New("Expected path to config file, got nothing")
		return configFile, client, err
	}

	// loading the config-file
	configFile, err = Config.New(args[1])
	if err != nil {
		return configFile, client, err
	}

	// connecting the redis clients
	client, err = Config.NewCacheClient(configFile.ConnectionList)
	if err != nil {
		return configFile, client, err
	}

	// flushing all of the databases
	err = client.FlushDb()
	if err != nil {
		return configFile, client, err
	}

	return configFile, client, nil
}

func load(client Cache.Client, configRegions []Config.Region) ([]Entity.Region, error) {
	var (
		regions []Entity.Region
		err     error
	)
	regionManager := Entity.RegionManager{Client: client}
	localeManager := Entity.LocaleManager{Client: client}

	for _, configRegion := range configRegions {
		region := configRegion.ToEntity()
		region, err = regionManager.Persist(region)
		if err != nil {
			return regions, err
		}

		for _, configLocale := range configRegion.Locales {
			locale := configLocale.ToEntity()
			locale.Region = region
			locale, err = localeManager.Persist(locale)
			if err != nil {
				return regions, err
			}
		}
		regions = append(regions, region)
	}
	return regions, nil
}

func getRealms(client Cache.Client, regions []Entity.Region) (map[int64][]Entity.Realm, error) {
	var (
		regionRealms map[int64][]Entity.Realm
		err          error
	)
	regionRealms = map[int64][]Entity.Realm{}
	realmManager := Entity.RealmManager{Client: client}

	// going over the regions to download the statuses
	c := make(chan Status.Result, len(regions))
	for _, region := range regions {
		go Status.Get(region, c)
	}

	// gathering the results
	results := make([]Status.Result, len(regions))
	for i := 0; i < len(results); i++ {
		results[i] = <-c
	}

	// going over the results
	for _, result := range results {
		if err = result.Error; err != nil {
			return regionRealms, err
		}

		region := result.Region
		statusRealms := result.Status.Realms
		for _, statusRealm := range statusRealms {
			realm := statusRealm.ToEntity()
			realm.Region = region
			realm, err = realmManager.Persist(realm)
			if err != nil {
				return regionRealms, err
			}

			regionRealms[region.Id] = append(regionRealms[region.Id], realm)
		}
	}

	return regionRealms, nil
}

func main() {
	Util.Write("Starting...")

	var (
		err          error
		client       Cache.Client
		configFile   Config.ConfigFile
		regions      []Entity.Region
		regionRealms map[int64][]Entity.Realm
	)

	/*
		reading the config
	*/
	// getting a client
	Util.Write("Initializing the config...")
	configFile, client, err = initialize(os.Args)
	if err != nil {
		Util.Write(fmt.Sprintf("initialize() fail: %s", err.Error()))
		return
	}

	// loading the regions and locales
	Util.Write("Loading the regions and locales...")
	regions, err = load(client, configFile.Regions)
	if err != nil {
		Util.Write(fmt.Sprintf("load() fail: %s", err.Error()))
		return
	}

	/*
		gathering and persisting realms for each region
	*/
	Util.Write("Fetching realms for each region...")
	regionRealms, err = getRealms(client, regions)
	if err != nil {
		Util.Write(fmt.Sprintf("getRealms() fail: %s", err.Error()))
		return
	}

	/*
		debug
	*/
	realmCount := 0
	for _, realms := range regionRealms {
		realmCount += len(realms)
	}
	Util.Write(fmt.Sprintf("Persisted %d realms...", realmCount))

	Util.Conclude()
}
