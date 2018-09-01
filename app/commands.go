package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/subjects"
	"github.com/ihsw/sotah-server/app/util"
	log "github.com/sirupsen/logrus"
)

func api(c config, m messenger, s store) error {
	log.Info("Starting api")

	// establishing a state
	res := newResolver(c, m, s)
	sta := newState(m, res)

	// ensuring cache-dirs exist
	cacheDirs := []string{
		fmt.Sprintf("%s/auctions", c.CacheDir),
		fmt.Sprintf("%s/items", c.CacheDir),
		fmt.Sprintf("%s/databases", c.CacheDir),
	}
	for _, reg := range sta.regions {
		cacheDirs = append(cacheDirs, fmt.Sprintf("%s/auctions/%s", c.CacheDir, reg.Name))
		cacheDirs = append(cacheDirs, fmt.Sprintf("%s/databases/%s", c.CacheDir, reg.Name))
	}
	if err := util.EnsureDirsExist(cacheDirs); err != nil {
		return err
	}

	// filling state with region statuses
	for _, reg := range sta.regions {
		if c.Whitelist[reg.Name] != nil && len(*c.Whitelist[reg.Name]) == 0 {
			log.WithField("region", reg.Name).Info("Filtering out region from status")

			continue
		}

		regionStatus, err := reg.getStatus(res)
		if err != nil {
			log.WithFields(log.Fields{
				"region": reg.Name,
				"error":  err.Error(),
			}).Info("Failed to fetch status")

			return err
		}

		sta.statuses[reg.Name] = regionStatus
	}

	// loading up items
	loadedItems, err := loadItemsFromFilecache(*res.config)
	if err != nil {
		return err
	}
	for job := range loadedItems {
		if job.err != nil {
			log.WithFields(log.Fields{
				"filepath": job.filepath,
				"error":    job.err.Error(),
			}).Error("Failed to load item")

			return job.err
		}

		sta.items[job.item.ID] = item{job.item, job.iconURL}
	}

	// gathering item-classes
	primaryRegion, err := c.Regions.getPrimaryRegion()
	if err != nil {
		return err
	}
	uri, err := res.appendAPIKey(res.getItemClassesURL(primaryRegion.Hostname))
	if err != nil {
		return err
	}
	iClasses, resp, err := blizzard.NewItemClassesFromHTTP(uri)
	if err != nil {
		return err
	}
	sta.itemClasses = iClasses
	if err := sta.messenger.publishPlanMetaMetric(resp); err != nil {
		return err
	}

	// gathering profession icons into storage
	if c.UseGCloudStorage {
		iconNames := make([]string, len(c.Professions))
		for i, prof := range c.Professions {
			iconNames[i] = prof.Icon
		}

		syncedIcons, err := s.syncItemIcons(iconNames, res)
		if err != nil {
			return err
		}
		for job := range syncedIcons {
			if job.err != nil {
				return job.err
			}

			for i, prof := range c.Professions {
				if prof.Icon != job.iconName {
					continue
				}

				c.Professions[i].IconURL = job.iconURL
			}
		}
	} else {
		for i, prof := range c.Professions {
			c.Professions[i].IconURL = defaultGetItemIconURL(prof.Icon)
		}
	}

	// opening all listeners
	sta.listeners = newListeners(subjectListeners{
		subjects.GenericTestErrors: sta.listenForGenericTestErrors,
		subjects.Status:            sta.listenForStatus,
		subjects.Regions:           sta.listenForRegions,
		subjects.Owners:            sta.listenForOwners,
		subjects.ItemsQuery:        sta.listenForItemsQuery,
		subjects.ItemClasses:       sta.listenForItemClasses,
		subjects.Items:             sta.listenForItems,
		subjects.Boot:              sta.listenForBoot,
	})
	if err := sta.listeners.listen(); err != nil {
		return err
	}

	// starting up a collector
	collectorStop := make(workerStopChan)
	onCollectorStop := sta.startCollector(collectorStop, res)

	// catching SIGINT
	sigIn := make(chan os.Signal, 1)
	signal.Notify(sigIn, os.Interrupt)
	<-sigIn

	log.Info("Caught SIGINT, exiting")

	// stopping listeners
	sta.listeners.stop()

	log.Info("Stopping collector")
	collectorStop <- struct{}{}

	log.Info("Waiting for collector to stop")
	<-onCollectorStop

	log.Info("Exiting")
	return nil
}
