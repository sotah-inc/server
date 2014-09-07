package Misc

import (
	"errors"
	"github.com/ihsw/go-download/Blizzard/Status"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Config"
	"github.com/ihsw/go-download/Entity"
	"github.com/vmihailenco/redis/v2"
)

/*
	funcs
*/
func newCacheWrapper(c Config.Connection) (w Cache.Wrapper, err error) {
	r := redis.NewTCPClient(&redis.Options{
		Addr:     c.Host,
		Password: c.Password,
		DB:       c.Db,
	})

	ping := r.Ping()
	if err = ping.Err(); err != nil {
		return
	}

	w = Cache.Wrapper{
		Redis: r,
	}
	return
}

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
	regionManager := Entity.RegionManager{Client: client}
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

func GetCacheClientAndRegions(args []string) (cacheClient Cache.Client, regions []Entity.Region, err error) {
	if len(args) == 1 {
		err = errors.New("Expected path to config file, got nothing")
		return
	}

	// loading the config-file
	var configFile Config.ConfigFile
	configFile, err = Config.NewConfigFile(args[1])
	if err != nil {
		return
	}

	// connecting the redis clients
	cacheClient, err = newCacheClient(configFile.ConnectionList)
	if err != nil {
		return
	}

	// flushing all of the databases
	err = cacheClient.FlushDb()
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
