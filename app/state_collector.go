package main

import (
	"encoding/json"
	"time"

	"github.com/ihsw/sotah-server/app/logging"
	"github.com/ihsw/sotah-server/app/subjects"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/sirupsen/logrus"
)

func (sta state) startCollector(stopChan workerStopChan, res resolver) workerStopChan {
	// collecting regions once
	sta.collectRegions(res)

	onStop := make(workerStopChan)
	go func() {
		ticker := time.NewTicker(20 * time.Minute)

		logging.Info("Starting collector")
	outer:
		for {
			select {
			case <-ticker.C:
				sta.collectRegions(res)
			case <-stopChan:
				ticker.Stop()

				break outer
			}
		}

		onStop <- struct{}{}
	}()

	return onStop
}

func (sta state) collectRegions(res resolver) {
	logging.Info("Collecting regions")

	// going over the list of regions
	startTime := time.Now()
	irData := intakeRequestData{}
	for _, reg := range sta.regions {
		// gathering whitelist for this region
		wList := res.config.getRegionWhitelist(reg.Name)
		if wList != nil && len(*wList) == 0 {
			continue
		}

		// misc
		regionItemIDsMap := map[blizzard.ItemID]struct{}{}
		irData[reg.Name] = map[blizzard.RealmSlug]int64{}

		// downloading auctions in a region
		logging.WithFields(logrus.Fields{
			"region":    reg.Name,
			"realms":    len(sta.statuses[reg.Name].Realms),
			"whitelist": wList,
		}).Debug("Downloading region")
		auctionsOut := sta.statuses[reg.Name].Realms.getAuctionsOrAll(sta.resolver, wList)
		for job := range auctionsOut {
			result, err := sta.auctionsIntake(job)
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": reg.Name,
					"realm":  job.realm.Slug,
				}).Error("Failed to intake auctions")
			}

			irData[reg.Name][job.realm.Slug] = job.lastModified.Unix()
			for _, ID := range result.itemIds {
				_, ok := sta.items[ID]
				if ok {
					continue
				}

				regionItemIDsMap[ID] = struct{}{}
			}
		}
		logging.WithField("region", reg.Name).Debug("Downloaded region")

		// optionally gathering the list of item IDs for this region
		if len(regionItemIDsMap) > 0 {
			regionItemIDs := make([]blizzard.ItemID, len(regionItemIDsMap))
			i := 0
			for ID := range regionItemIDsMap {
				regionItemIDs[i] = ID
				i++
			}

			// downloading items found in this region
			logging.WithField("items", len(regionItemIDs)).Info("Fetching items")
			itemsOut := getItems(regionItemIDs, res)
			for job := range itemsOut {
				if job.err != nil {
					logging.WithFields(logrus.Fields{
						"error":  job.err.Error(),
						"region": reg.Name,
						"item":   job.ID,
					}).Error("Failed to fetch item")

					continue
				}

				sta.items[job.ID] = item{job.item, job.iconURL}
			}
			logging.WithField("items", len(regionItemIDs)).Debug("Fetched items")
		}
	}

	// publishing for intake into live auctions
	aiRequest := auctionsIntakeRequest{irData}
	encodedAiRequest, err := json.Marshal(aiRequest)
	if err != nil {
		logging.WithField("error", err.Error()).Error("Failed to marshal auctions-intake-request")
	} else {
		res.messenger.publish(subjects.AuctionsIntake, encodedAiRequest)
	}

	// re-syncing all item icons
	iconsMap := sta.items.getItemIconsMap(true)
	logging.WithField("items", len(iconsMap)).Info("Syncing item-icons")
	if res.config.UseGCloudStorage {
		itemIconsOut, err := res.store.syncItemIcons(iconsMap.getItemIcons(), res)
		if err != nil {
			logging.WithField("error", err.Error()).Error("Failed to start syncing item-icons")
		} else {
			for job := range itemIconsOut {
				if job.err != nil {
					logging.WithFields(logrus.Fields{
						"error":    job.err.Error(),
						"iconName": job.iconName,
					}).Error("Failed to sync item icon")

					continue
				}

				for _, itemID := range iconsMap[job.iconName] {
					if sta.items[itemID].IconURL != "" {
						continue
					}

					itemValue := sta.items[itemID]
					itemValue.IconURL = job.iconURL
					sta.items[itemID] = itemValue
				}
			}
		}
	} else {
		itemIconsOut := syncItemIcons(iconsMap.getItemIcons(), res)
		for job := range itemIconsOut {
			if job.err != nil {
				logging.WithFields(logrus.Fields{
					"error": job.err.Error(),
					"item":  job.icon,
				}).Error("Failed to sync item icon")

				continue
			}
		}
	}
	logging.WithField("items", len(iconsMap)).Info("Synced item-icons")

	sta.messenger.publishMetric(telegrafMetrics{
		"collector_duration": int64(time.Now().Unix() - startTime.Unix()),
	})
}
