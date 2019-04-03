package state

import (
	"encoding/json"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
)

func (sta APIState) StartCollector(stopChan sotah.WorkerStopChan) sotah.WorkerStopChan {
	sta.collectRegions()

	onStop := make(sotah.WorkerStopChan)
	go func() {
		ticker := time.NewTicker(20 * time.Minute)

		logging.Info("Starting collector")
	outer:
		for {
			select {
			case <-ticker.C:
				// refreshing the access-token for the Resolver blizz client
				nextClient, err := sta.IO.Resolver.BlizzardClient.RefreshFromHTTP(blizzard.OAuthTokenEndpoint)
				if err != nil {
					logging.WithField("error", err.Error()).Error("Failed to refresh blizzard client")

					continue
				}
				sta.IO.Resolver.BlizzardClient = nextClient

				sta.collectRegions()
			case <-stopChan:
				ticker.Stop()

				break outer
			}
		}

		onStop <- struct{}{}
	}()

	return onStop
}

func (sta APIState) collectRegions() {
	logging.Info("Collecting regions")

	// for subsequently pushing to the live-auctions-intake listener
	regionRealmTimestamps := sotah.RegionRealmTimestamps{}

	// going over the list of regions
	startTime := time.Now()
	totalRealms := 0
	includedRealmCount := 0
	for regionName, status := range sta.Statuses {
		totalRealms += len(status.Realms)

		// misc
		receivedItemIds := map[blizzard.ItemID]struct{}{}

		// starting channels for persisting auctions
		storeAuctionsInJobs := make(chan StoreAuctionsInJob)
		storeAuctionsOutJobs := sta.StoreAuctions(storeAuctionsInJobs)

		// queueing up the jobs
		go func() {
			logging.WithFields(logrus.Fields{
				"region": regionName,
				"realms": len(status.Realms),
			}).Debug("Downloading region")
			for getAuctionsJob := range sta.IO.Resolver.GetAuctionsForRealms(status.Realms) {
				if getAuctionsJob.Err != nil {
					logrus.WithFields(getAuctionsJob.ToLogrusFields()).Error("Failed to fetch auctions")

					continue
				}

				storeAuctionsInJobs <- StoreAuctionsInJob{
					Realm:      getAuctionsJob.Realm,
					TargetTime: getAuctionsJob.LastModified,
					Auctions:   getAuctionsJob.Auctions,
				}
			}

			close(storeAuctionsInJobs)
		}()

		// waiting for the store-load-in jobs to drain out
		for job := range storeAuctionsOutJobs {
			// incrementing included-realm count
			includedRealmCount++

			// optionally skipping on error
			if job.Err != nil {
				logging.WithFields(job.ToLogrusFields()).Error("Failed to persist auctions")

				continue
			}

			if _, ok := regionRealmTimestamps[job.Realm.Region.Name]; !ok {
				regionRealmTimestamps[job.Realm.Region.Name] = sotah.RealmTimestamps{}
			}
			regionRealmTimestamps[job.Realm.Region.Name][job.Realm.Slug] = job.TargetTime.Unix()

			// updating the realm last-modified in statuses
			for i, statusRealm := range status.Realms {
				if statusRealm.Slug != job.Realm.Slug {
					continue
				}

				sta.Statuses[job.Realm.Region.Name].Realms[i].LastModified = job.TargetTime.Unix()

				break
			}

			// appending to received item-ids
			for _, itemId := range job.ItemIds {
				receivedItemIds[itemId] = struct{}{}
			}
		}
		logging.WithField("region", regionName).Debug("Downloaded and persisted region")

		// resolving items
		err := func() error {
			logging.Debug("Fetching items from database")

			// gathering current items
			iMap, err := sta.IO.Databases.ItemsDatabase.GetItems()
			if err != nil {
				return err
			}

			// gathering new item-ids
			newItemIds := func() []blizzard.ItemID {
				out := []blizzard.ItemID{}

				for ID := range receivedItemIds {
					if sta.ItemBlacklist.IsPresent(ID) {
						continue
					}

					if _, ok := iMap[ID]; ok {
						continue
					}

					out = append(out, ID)
				}

				return out
			}()

			// flag for determining whether the full items map should be persisted
			hasNewResults := false

			// gathering new items
			iMap, hasNewResults, err = func(inItemsMap sotah.ItemsMap, inHasNewResults bool) (sotah.ItemsMap, bool, error) {
				if len(newItemIds) == 0 {
					return inItemsMap, inHasNewResults, nil
				}

				logging.WithField("items", len(newItemIds)).Debug("Resolving new items")

				primaryRegion, err := sta.Regions.GetPrimaryRegion()
				if err != nil {
					return sotah.ItemsMap{}, false, err
				}

				getItemsJobs := sta.IO.Resolver.GetItems(primaryRegion, newItemIds)
				for job := range getItemsJobs {
					if job.Err != nil {
						logging.WithFields(logrus.Fields{
							"error":   job.Err.Error(),
							"item-id": job.ItemId,
						}).Error("Failed to fetch item")

						continue
					}

					inHasNewResults = true

					inItemsMap[job.ItemId] = sotah.Item{
						Item:    job.Item,
						IconURL: "",
					}
				}

				return inItemsMap, inHasNewResults, nil
			}(iMap, hasNewResults)
			if err != nil {
				return err
			}

			// gathering missing item icons
			iMap, hasNewResults, err = func(inItemsMap sotah.ItemsMap, inHasNewResults bool) (sotah.ItemsMap, bool, error) {
				missingItemIcons := inItemsMap.GetItemIconsMap(true)

				// optionally halting on no missing item-icons
				if len(missingItemIcons) == 0 {
					return inItemsMap, inHasNewResults, nil
				}

				// optionally halting on non-gcloud environment
				if !sta.UseGCloud {
					for iconName, IDs := range missingItemIcons {
						for _, ID := range IDs {
							itemValue := inItemsMap[ID]
							itemValue.IconURL = blizzard.DefaultGetItemIconURL(iconName)
							inItemsMap[ID] = itemValue
						}
					}

					return inItemsMap, inHasNewResults, nil
				}

				// starting channels for persisting item-icons
				persistItemIconsInJobs := make(chan store.PersistItemIconsInJob)
				persistItemIconsOutJobs := sta.IO.StoreClient.PersistItemIcons(persistItemIconsInJobs)

				// queueing up the jobs
				go func() {
					for outJob := range sta.IO.Resolver.GetItemIcons(missingItemIcons.GetItemIcons()) {
						if outJob.Err != nil {
							logging.WithFields(outJob.ToLogrusFields()).Error("Failed to fetch item-icon")

							continue
						}

						persistItemIconsInJobs <- store.PersistItemIconsInJob{
							IconName: outJob.IconName,
							Data:     outJob.Data,
						}
					}

					close(persistItemIconsInJobs)
				}()

				// gathering results for persistence
				for job := range persistItemIconsOutJobs {
					inHasNewResults = true

					for _, itemId := range missingItemIcons[job.IconName] {
						item := inItemsMap[itemId]
						item.IconURL = job.IconURL
						inItemsMap[itemId] = item
					}
				}

				return inItemsMap, inHasNewResults, nil
			}(iMap, hasNewResults)
			if err != nil {
				return err
			}

			// optionally persisting
			if hasNewResults {
				if err := sta.IO.Databases.ItemsDatabase.PersistItems(iMap); err != nil {
					return err
				}
			}

			return nil
		}()
		if err != nil {
			logging.WithFields(logrus.Fields{
				"error":  err.Error(),
				"region": regionName,
			}).Error("Failed to process item-ids from batch")
		}
	}

	// publishing for live-auctions-intake
	iRequest := liveAuctionsIntakeRequest{RegionRealmTimestamps: regionRealmTimestamps}
	err := func() error {
		encodedRequest, err := json.Marshal(iRequest)
		if err != nil {
			return err
		}

		return sta.IO.Messenger.Publish(string(subjects.LiveAuctionsIntake), encodedRequest)
	}()
	if err != nil {
		logging.WithField("error", err.Error()).Error("Failed to publish live-auctions-intake-request")

		return
	}

	duration := time.Now().Sub(startTime)
	sta.IO.Reporter.Report(metric.Metrics{
		"auctionscollector_intake_duration": int(duration) / 1000 / 1000 / 1000,
		"included_realms":                   includedRealmCount,
		"excluded_realms":                   totalRealms - includedRealmCount,
		"total_realms":                      totalRealms,
	})
	logging.Info("Finished collector")
}
