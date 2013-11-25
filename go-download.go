package main

import (
	"errors"
	"fmt"
	"github.com/ihsw/go-download/Blizzard/Auction"
	"github.com/ihsw/go-download/Blizzard/Status"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Config"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Util"
	"os"
)

func Initialize(args []string) (Config.ConfigFile, Cache.Client, error) {
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

func Load(client Cache.Client, configRegions []Config.Region) ([]Entity.Region, error) {
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
		responseRealms := result.Response.Realms
		for _, responseRealm := range responseRealms {
			realm := responseRealm.ToEntity()
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
	configFile, client, err = Initialize(os.Args)
	if err != nil {
		Util.Write(fmt.Sprintf("initialize() fail: %s", err.Error()))
		return
	}

	// loading the regions and locales
	Util.Write("Loading the regions and locales...")
	regions, err = Load(client, configFile.Regions)
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
		removing realms that aren't queryable
	*/
	totalRealms := 0
	for _, region := range regions {
		if !region.Queryable {
			delete(regionRealms, region.Id)
			continue
		}
		totalRealms += len(regionRealms[region.Id])
	}

	/*
		checking the status of each realm
	*/
	Util.Write("Checking the status of each realm...")
	c := make(chan Auction.Result, totalRealms)

	// going over the realms for the initial polling
	Util.Write("Queueing up checking the realm status...")
	for _, realms := range regionRealms {
		for _, realm := range realms {
			go Auction.Get(realm, c)
		}
	}

	// gathering the results
	Util.Write("Gathering the results...")
	results := make([]Auction.Result, totalRealms)
	for i := 0; i < totalRealms; i++ {
		results[i] = <-c
	}

	// going over the results
	Util.Write("Going over the results...")
	count := 0
	size := int64(0)
	for _, result := range results {
		if err = result.Error; err != nil {
			Util.Write(fmt.Sprintf("Auction.Get() fail: %s", err.Error()))
			return
		}
		count++
		size += result.Length
	}
	Util.Write(fmt.Sprintf("Count: %d, size: %.2f MB", count, float64(size)/1000/1000))

	Util.Conclude()
}
