package Misc

import (
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Config"
	"github.com/ihsw/go-download/Entity"
)

/*
	funcs
*/
func Init(configPath string, flushDb bool) (client Cache.Client, regions []Entity.Region, err error) {
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

	// gathering the regions
	regionManager := Entity.NewRegionManager(client)
	if flushDb {
		regions = make([]Entity.Region, len(configFile.Regions))
		for i, configRegion := range configFile.Regions {
			regions[i] = Region{
				Name:      configRegion.Name,
				Host:      configRegion.Host,
				Queryable: configRegion.Queryable,
			}
		}
		if regions, err = regionManager.PersistAll(regions); err != nil {
			return
		}
	} else {
		if regions, err = regionManager.FindAll(); err != nil {
			return
		}
	}

	return
}
