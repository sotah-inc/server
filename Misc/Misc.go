package Misc

import (
	"errors"
	"github.com/ihsw/go-download/Blizzard/Status"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Config"
	"github.com/ihsw/go-download/Entity"
	redis "gopkg.in/redis.v2"
)

/*
	funcs
*/

func newCacheClient(c Config.ConnectionList) (client Cache.Client, err error) {
	client.Main, err = newCacheWrapper(c.Main)
	if err != nil {
		return
	}

	var w Cache.Wrapper
	for _, poolItem := range c.Pool {
		w, err = newCacheWrapper(poolItem)
		if err != nil {
			return
		}
		client.Pool = append(client.Pool, w)
	}

	return client, nil
}

func getRegions(client Cache.Client, configRegions []Config.Region) (regions []Entity.Region, err error) {
	regionManager := Entity.NewRegionManager(client)
	localeManager := Entity.LocaleManager{Client: client}

	for _, configRegion := range configRegions {
		region := configRegion.ToEntity()
		region, err = regionManager.Persist(region)
		if err != nil {
			return
		}

		for _, configLocale := range configRegion.Locales {
			locale := configLocale.ToEntity()
			locale.Region = region
			locale, err = localeManager.Persist(locale)
			if err != nil {
				return
			}
		}
		regions = append(regions, region)
	}
	return regions, nil
}

func GetCacheClient(configPath string, flushDb bool) (cacheClient Cache.Client, configFile Config.ConfigFile, err error) {
	if len(configPath) == 0 {
		err = errors.New("Expected path to config file, got nothing")
		return
	}

	// opening the config
	if configFile, err = Config.New(configPath); err != nil {
		return
	}

	// connecting up the redis clients
	if cacheClient, err = newCacheClient(configFile.ConnectionList); err != nil {
		return
	}
	cacheClient.ApiKey = configFile.ApiKey

	// optionally flushing all of the databases
	if flushDb {
		err = cacheClient.FlushDb()
		if err != nil {
			return
		}
	}

	return cacheClient, configFile, nil
}

func GetCacheClientAndRegions(configPath string, flushDb bool) (cacheClient Cache.Client, regions []Entity.Region, err error) {
	var configFile Config.ConfigFile
	cacheClient, configFile, err = GetCacheClient(configPath, flushDb)
	if err != nil {
		return
	}

	// gathering the regions
	regions, err = getRegions(cacheClient, configFile.Regions)
	if err != nil {
		return
	}

	return cacheClient, regions, nil
}

func GetRealms(client Cache.Client, regions []Entity.Region) (map[int64][]Entity.Realm, error) {
	var (
		regionRealms map[int64][]Entity.Realm
		err          error
	)
	regionRealms = map[int64][]Entity.Realm{}
	realmManager := Entity.NewRealmManager(client)

	// going over the regions to download the statuses
	c := make(chan Status.Result, len(regions))
	for _, region := range regions {
		go Status.Get(region, client.ApiKey, c)
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
