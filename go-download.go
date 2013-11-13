package main

import (
	"fmt"
	"github.com/ihsw/go-download/Blizzard/Status"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Config"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Util"
	"os"
)

func main() {
	Util.Write("Starting...")

	var (
		err    error
		client Cache.Client
		config Config.Config
	)

	/*
		initialization
	*/
	if len(os.Args) == 1 {
		Util.Write("Expected path to config file, got nothing")
		return
	}

	// loading the config
	config, err = Config.New(os.Args[1])
	if err != nil {
		Util.Write(fmt.Sprintf("Config.New() fail: %s", err.Error()))
		return
	}

	// connecting the redis clients
	client, err = Cache.NewClient(config.Redis_Config)
	if err != nil {
		Util.Write(fmt.Sprintf("Cache.NewClient() fail: %s", err.Error()))
		return
	}

	// flushing all of the databases
	err = client.FlushDb()
	if err != nil {
		Util.Write(fmt.Sprintf("client.FlushDb() fail: %s", err.Error()))
		return
	}

	/*
		reading the config
	*/
	// managers
	regionManager := Entity.RegionManager{Client: client}
	localeManager := Entity.LocaleManager{Client: client}

	// persisting the regions and locales
	Util.Write(fmt.Sprintf("Persisting %d regions...", len(config.Regions)))
	regions := map[int64]Entity.Region{}
	for _, configRegion := range config.Regions {
		region := Entity.NewRegionFromConfig(configRegion)
		region, err = regionManager.Persist(region)
		if err != nil {
			Util.Write(fmt.Sprintf("regionManager.Persist() fail: %s", err.Error()))
			return
		}

		Util.Write(fmt.Sprintf("Persisting %d locales belonging to %s...", len(configRegion.Locales), region.Name))
		for _, configLocale := range configRegion.Locales {
			locale := Entity.NewLocaleFromConfig(configLocale)
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
		go Status.Get(region.Host, region.Id, c)
	}

	// gathering the results
	Util.Write(fmt.Sprintf("Gathering the results for %d regions...", len(regions)))
	results := make([]Status.Result, len(regions))
	for i := 0; i < len(results); i++ {
		results[i] = <-c
	}

	// going over the results
	Util.Write(fmt.Sprintf("Going over %d results...", len(results)))
	for _, result := range results {
		if err = result.Error; err != nil {
			Util.Write(fmt.Sprintf("Status.Get() fail: %s", err.Error()))
			return
		}

		region := regions[result.RegionId]

		for _, statusRealm := range result.Status.Realms {
			Util.Write(fmt.Sprintf("%s-%s is type %s", region.Name, statusRealm.Slug, statusRealm.RealmType))
		}

		Util.Write(fmt.Sprintf("Region %s has %d realms", region.Name, len(result.Status.Realms)))
	}

	Util.Conclude()
}
