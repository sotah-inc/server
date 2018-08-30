package main

import (
	"encoding/json"
	"time"

	"github.com/ihsw/sotah-server/app/subjects"

	"github.com/ihsw/sotah-server/app/blizzard"
	log "github.com/sirupsen/logrus"
)

func (sta state) startCollector(stopChan workerStopChan, res resolver) workerStopChan {
	// collecting regions once
	sta.collectRegions(res)

	onStop := make(workerStopChan)
	go func() {
		ticker := time.NewTicker(10 * time.Minute)

		log.Info("Starting collector")
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
	log.Info("Collecting regions")

	// going over the list of regions
	startTime := time.Now()
	collectedRegionRealmSlugs := map[regionName][]blizzard.RealmSlug{}
	for _, reg := range sta.regions {
		// gathering whitelist for this region
		wList := res.config.getRegionWhitelist(reg.Name)
		if wList != nil && len(*wList) == 0 {
			continue
		}

		// misc
		regionItemIDsMap := map[blizzard.ItemID]struct{}{}
		collectedRegionRealmSlugs[reg.Name] = []blizzard.RealmSlug{}

		// downloading auctions in a region
		log.WithFields(log.Fields{
			"region":    reg.Name,
			"realms":    len(sta.statuses[reg.Name].Realms),
			"whitelist": wList,
		}).Info("Downloading region")
		auctionsOut := sta.statuses[reg.Name].Realms.getAuctionsOrAll(sta.resolver, wList)
		for job := range auctionsOut {
			result, err := sta.auctionsIntake(job)
			if err != nil {
				log.WithFields(log.Fields{
					"region": reg.Name,
					"realm":  job.realm.Slug,
					"error":  err.Error(),
				}).Info("Failed to intake auctions")
			}

			collectedRegionRealmSlugs[reg.Name] = append(collectedRegionRealmSlugs[reg.Name], job.realm.Slug)
			for _, ID := range result.itemIds {
				_, ok := sta.items[ID]
				if ok {
					continue
				}

				regionItemIDsMap[ID] = struct{}{}
			}
		}
		log.WithField("region", reg.Name).Info("Downloaded region")

		// optionally gathering the list of item IDs for this region
		if len(regionItemIDsMap) > 0 {
			regionItemIDs := make([]blizzard.ItemID, len(regionItemIDsMap))
			i := 0
			for ID := range regionItemIDsMap {
				regionItemIDs[i] = ID
				i++
			}

			// downloading items found in this region
			log.WithField("items", len(regionItemIDs)).Info("Fetching items")
			itemsOut := getItems(regionItemIDs, res)
			for job := range itemsOut {
				if job.err != nil {
					log.WithFields(log.Fields{
						"region": reg.Name,
						"item":   job.ID,
						"error":  job.err.Error(),
					}).Info("Failed to fetch item")

					continue
				}

				sta.items[job.ID] = item{job.item, job.iconURL}
			}
			log.WithField("items", len(regionItemIDs)).Info("Fetched items")
		}
	}

	// publishing for intake into live auctions
	aiRequest := auctionsIntakeRequest{collectedRegionRealmSlugs}
	encodedAiRequest, err := json.Marshal(aiRequest)
	if err != nil {
		log.WithField("error", err.Error()).Info("Failed to marshal auctions-intake-request")
	} else {
		res.messenger.publish(subjects.AuctionsIntake, encodedAiRequest)
	}

	// re-syncing all item icons
	iconsMap := sta.items.getItemIconsMap(true)
	log.WithField("items", len(iconsMap)).Info("Syncing item-icons")
	if res.config.UseGCloudStorage {
		itemIconsOut, err := res.store.syncItemIcons(iconsMap.getItemIcons(), res)
		if err != nil {
			log.WithField("error", err.Error()).Info("Failed to start syncing item-icons")
		}

		for job := range itemIconsOut {
			if job.err != nil {
				log.WithFields(log.Fields{
					"iconName": job.iconName,
					"error":    job.err.Error(),
				}).Info("Failed to sync item icon")

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
	} else {
		itemIconsOut := syncItemIcons(iconsMap.getItemIcons(), res)
		for job := range itemIconsOut {
			if job.err != nil {
				log.WithFields(log.Fields{
					"item":  job.icon,
					"error": job.err.Error(),
				}).Info("Failed to sync item icon")

				continue
			}
		}
	}
	log.WithField("items", len(iconsMap)).Info("Synced item-icons")

	sta.messenger.publishMetric(telegrafMetrics{
		"collector_duration": int64(time.Now().Unix() - startTime.Unix()),
	})
}
