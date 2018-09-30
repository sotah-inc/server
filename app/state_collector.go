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
		receivedItemIds := map[blizzard.ItemID]struct{}{}
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

				continue
			}

			irData[reg.Name][job.realm.Slug] = job.lastModified.Unix()
			for _, ID := range result.itemIds {
				receivedItemIds[ID] = struct{}{}
			}
		}
		logging.WithField("region", reg.Name).Debug("Downloaded region")

		// resolving items
		err := func() error {
			iMap, err := sta.itemsDatabase.getItems()
			if err != nil {
				return err
			}

			newItemIds := func() []blizzard.ItemID {
				out := []blizzard.ItemID{}

				for ID := range receivedItemIds {
					if _, ok := iMap[ID]; ok {
						continue
					}

					out = append(out, ID)
				}

				return out
			}()

			itemsOut := getItems(newItemIds, sta.itemBlacklist, res)
			for itemsOutJob := range itemsOut {
				if itemsOutJob.err != nil {
					logging.WithFields(logrus.Fields{
						"error": err.Error(),
						"ID":    itemsOutJob.ID,
					}).Error("Failed to fetch item")

					continue
				}

				iMap[itemsOutJob.ID] = item{itemsOutJob.item, ""}
			}

			if err := sta.itemsDatabase.persistItems(iMap); err != nil {
				return err
			}

			return nil
		}()
		if err != nil {
			logging.WithFields(logrus.Fields{
				"error":  err.Error(),
				"region": reg.Name,
			}).Error("Failed to process item-ids from batch")
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
