package Misc

import (
	"github.com/ihsw/go-download/Blizzard/Status"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Config"
	"github.com/ihsw/go-download/Entity"
)

/*
	funcs
*/
func Init(configPath string, flushDb bool) (client Cache.Client, regions []Entity.Region, regionRealms map[int64][]Entity.Realm, err error) {
	regionRealms = make(map[int64][]Entity.Realm)

	// opening the config file
	var configFile Config.File
	if configFile, err = Config.New(configPath); err != nil {
		return
	}

	// connecting the client
	if client, err = Cache.NewClient(configFile); err != nil {
		return
	}
	if flushDb {
		if err = client.FlushDb(); err != nil {
			return
		}
	}

	// gathering the regions and realms
	regionManager := Entity.NewRegionManager(client)
	realmManager := Entity.NewRealmManager(client)
	if flushDb {
		regions = make([]Entity.Region, len(configFile.Regions))
		for i, configRegion := range configFile.Regions {
			regions[i] = Entity.Region{
				Name:      configRegion.Name,
				Host:      configRegion.Host,
				Queryable: configRegion.Queryable,
			}
		}
		if regions, err = regionManager.PersistAll(regions); err != nil {
			return
		}

		for _, region := range regions {
			var response Status.Response
			if response, err = Status.Get(region, client.ApiKey); err != nil {
				return
			}

			realms := make([]Entity.Realm, len(response.Realms))
			for i, responseRealm := range response.Realms {
				realms[i] = Entity.Realm{
					Name:        responseRealm.Name,
					Slug:        responseRealm.Slug,
					Battlegroup: responseRealm.Battlegroup,
					Type:        responseRealm.Type,
					Status:      responseRealm.Status,
					Population:  responseRealm.Population,
					Region:      region,
				}
			}
			if regionRealms[region.Id], err = realmManager.PersistAll(realms); err != nil {
				return
			}
		}
	} else {
		if regions, err = regionManager.FindAll(); err != nil {
			return
		}
		for _, region := range regions {
			if regionRealms[region.Id], err = realmManager.FindByRegion(region); err != nil {
				return
			}
		}
	}

	return
}
