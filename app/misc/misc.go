package misc

import (
	"github.com/ihsw/go-download/app/cache"
	"github.com/ihsw/go-download/app/config"
	"github.com/ihsw/go-download/app/entity"
)

/*
	funcs
*/
func Init(configPath string, flushDb bool) (client cache.Client, regions []entity.Region, regionRealms map[int64][]entity.Realm, err error) {
	// opening the config file
	var configFile Config.File
	if configFile, err = Config.New(configPath); err != nil {
		return
	}

	// connecting the client
	if client, err = cache.NewClient(configFile); err != nil {
		return
	}
	if flushDb {
		if err = client.FlushDb(); err != nil {
			return
		}
	}

	// gathering the regions and realms
	regionRealms = make(map[int64][]entity.Realm)
	regionManager := entity.NewRegionManager(client)
	if flushDb {
		regions = make([]entity.Region, len(configFile.Regions))
		for i, configRegion := range configFile.Regions {
			regions[i] = entity.Region{
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

			realms := make([]entity.Realm, len(response.Realms))
			for i, responseRealm := range response.Realms {
				realms[i] = entity.Realm{
					Name:        responseRealm.Name,
					Slug:        responseRealm.Slug,
					Battlegroup: responseRealm.Battlegroup,
					Type:        responseRealm.Type,
					Status:      responseRealm.Status,
					Population:  responseRealm.Population,
					Region:      region,
				}
			}
			realmManager := entity.NewRealmManager(region, client)
			if regionRealms[region.Id], err = realmManager.PersistAll(realms); err != nil {
				return
			}
		}
	} else {
		if regions, err = regionManager.FindAll(); err != nil {
			return
		}
		for _, region := range regions {
			realmManager := entity.NewRealmManager(region, client)
			if regionRealms[region.Id], err = realmManager.FindAll(); err != nil {
				return
			}
		}
	}

	return
}
