package state

import (
	"encoding/json"
	"time"

	"github.com/sotah-inc/server/app/metric"

	"github.com/sotah-inc/server/app/logging"
	"github.com/sotah-inc/server/app/subjects"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/blizzard"
)

func (sta State) startCollector(stopChan WorkerStopChan, res resolver) WorkerStopChan {
	onStop := make(WorkerStopChan)
	go func() {
		ticker := time.NewTicker(20 * time.Minute)

		logging.Info("Starting collector")
	outer:
		for {
			select {
			case <-ticker.C:
				// refreshing the access-token for the resolver blizz client
				nextClient, err := res.blizzardClient.RefreshFromHTTP(blizzard.OAuthTokenEndpoint)
				if err != nil {
					logging.WithField("error", err.Error()).Error("Failed to refresh blizzard client")

					continue
				}
				res.blizzardClient = nextClient
				sta.resolver = res

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

func (sta State) collectRegions(res resolver) {
	logging.Info("Collecting Regions")

	// going over the list of Regions
	startTime := time.Now()
	irData := IntakeRequestData{}
	totalRealms := 0
	includedRealmCount := 0
	for _, reg := range sta.Regions {
		// gathering whitelist for this region
		wList := res.config.getRegionWhitelist(reg.Name)
		if wList != nil && len(*wList) == 0 {
			continue
		}

		totalRealms += len(sta.Statuses[reg.Name].Realms.filterWithWhitelist(wList))

		// misc
		receivedItemIds := map[blizzard.ItemID]struct{}{}
		irData[reg.Name] = map[blizzard.RealmSlug]int64{}

		// downloading auctions in a region
		logging.WithFields(logrus.Fields{
			"region":    reg.Name,
			"realms":    len(sta.Statuses[reg.Name].Realms),
			"whitelist": wList,
		}).Debug("Downloading region")
		auctionsOut := sta.Statuses[reg.Name].Realms.getAuctionsOrAll(sta.resolver, wList)
		for job := range auctionsOut {
			result, err := sta.auctionsIntake(job)
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": reg.Name,
					"Realm":  job.realm.Slug,
				}).Error("Failed to intake auctions")

				continue
			}

			includedRealmCount++

			irData[reg.Name][job.realm.Slug] = job.lastModified.Unix()
			for _, ID := range result.itemIds {
				receivedItemIds[ID] = struct{}{}
			}
		}
		logging.WithField("region", reg.Name).Debug("Downloaded region")

		// resolving items
		err := func() error {
			logging.Debug("Fetching items from database")

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

			hasNewResults := false

			if len(newItemIds) > 0 {
				hasNewResults = true

				logging.WithField("items", len(newItemIds)).Debug("Resolving new items")

				itemsOut := getItems(newItemIds, sta.itemBlacklist, res)
				for itemsOutJob := range itemsOut {
					if itemsOutJob.err != nil {
						logging.WithFields(logrus.Fields{
							"error": itemsOutJob.err.Error(),
							"ID":    itemsOutJob.ID,
						}).Error("Failed to fetch item")

						continue
					}

					iMap[itemsOutJob.ID] = item{itemsOutJob.item, ""}
				}
			}

			missingItemIcons := iMap.getItemIconsMap(true)
			if len(missingItemIcons) > 0 {
				hasNewResults = true

				logging.WithField("icons", len(missingItemIcons)).Debug("Gathering item icons")
				if !res.config.UseGCloud {
					for iconName, IDs := range missingItemIcons {
						for _, ID := range IDs {
							itemValue := iMap[ID]
							itemValue.IconURL = defaultGetItemIconURL(iconName)
							iMap[ID] = itemValue
						}
					}
				} else {
					iconSyncJobs, err := res.store.syncItemIcons(missingItemIcons.getItemIcons(), res)
					if err != nil {
						return err
					}

					for iconSyncJob := range iconSyncJobs {
						if iconSyncJob.err != nil {
							logging.WithFields(logrus.Fields{
								"error": err.Error(),
								"icon":  iconSyncJob.iconName,
							}).Error("Failed to sync item icon")

							continue
						}

						for _, ID := range missingItemIcons[iconSyncJob.iconName] {
							itemValue := iMap[ID]
							itemValue.IconURL = iconSyncJob.iconURL
							iMap[ID] = itemValue
						}
					}
				}
			}

			if hasNewResults {
				if err := sta.itemsDatabase.persistItems(iMap); err != nil {
					return err
				}
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
	aiRequest := AuctionsIntakeRequest{irData}
	err := func() error {
		encodedAiRequest, err := json.Marshal(aiRequest)
		if err != nil {
			return err
		}

		return res.messenger.publish(subjects.AuctionsIntake, encodedAiRequest)
	}()
	if err != nil {
		logging.WithField("error", err.Error()).Error("Failed to publish auctions-intake-request")
	}

	metric.ReportDuration(
		metric.CollectorDuration,
		metric.DurationMetrics{
			Duration:       time.Now().Sub(startTime),
			TotalRealms:    totalRealms,
			IncludedRealms: includedRealmCount,
			ExcludedRealms: totalRealms - includedRealmCount,
		},
		logrus.Fields{},
	)
	logging.Info("Finished collector")
}
