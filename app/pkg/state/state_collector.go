package state

import (
	"encoding/json"
	"time"

	"github.com/sotah-inc/server/app/pkg/store"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/sotah"
)

func (sta State) StartCollector(stopChan sotah.WorkerStopChan) sotah.WorkerStopChan {
	onStop := make(sotah.WorkerStopChan)
	go func() {
		ticker := time.NewTicker(20 * time.Minute)

		logging.Info("Starting collector")
	outer:
		for {
			select {
			case <-ticker.C:
				// refreshing the access-token for the Resolver blizz client
				nextClient, err := sta.IO.resolver.BlizzardClient.RefreshFromHTTP(blizzard.OAuthTokenEndpoint)
				if err != nil {
					logging.WithField("error", err.Error()).Error("Failed to refresh blizzard client")

					continue
				}
				sta.IO.resolver.BlizzardClient = nextClient

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

func (sta State) collectRegions() {
	logging.Info("Collecting Regions")

	// going over the list of Regions
	startTime := time.Now()
	totalRealms := 0
	includedRealmCount := 0
	for regionName, status := range sta.Statuses {
		totalRealms += len(status.Realms)

		// misc
		receivedItemIds := map[blizzard.ItemID]struct{}{}

		// starting channels for persisting auctions
		loadAuctionsInJobs := make(chan store.LoadAuctionsInJob)
		loadAuctionsOutJobs := sta.IO.store.LoadAuctions(loadAuctionsInJobs)

		// queueing up the jobs
		go func() {
			logging.WithFields(logrus.Fields{
				"region": regionName,
				"realms": len(status.Realms),
			}).Debug("Downloading region")
			for getAuctionsJob := range sta.IO.resolver.GetAuctionsForRealms(status.Realms) {
				if getAuctionsJob.Err != nil {
					logrus.WithFields(getAuctionsJob.ToLogrusFields()).Error("Failed to fetch auctions")

					continue
				}

				loadAuctionsInJobs <- store.LoadAuctionsInJob{
					Realm:      getAuctionsJob.Realm,
					TargetTime: getAuctionsJob.LastModified,
					Auctions:   getAuctionsJob.Auctions,
				}
			}

			close(loadAuctionsInJobs)
		}()

		// waiting for the store-load-in jobs to drain out
		for job := range loadAuctionsOutJobs {
			// incrementing included-realm count
			includedRealmCount++

			// optionally skipping on error
			if job.Err != nil {
				logging.WithFields(job.ToLogrusFields()).Error("Failed to persist auctions")

				continue
			}

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
			iMap, err := sta.IO.databases.ItemsDatabase.GetItems()
			if err != nil {
				return err
			}

			// gathering new item-ids
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

				getItemsJobs := sta.IO.resolver.GetItems(primaryRegion, newItemIds)
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
				persistItemIconsOutJobs, err := sta.IO.store.PersistItemIcons(persistItemIconsInJobs)
				if err != nil {
					close(persistItemIconsInJobs)

					return sotah.ItemsMap{}, false, err
				}

				// queueing up the jobs
				go func() {
					for outJob := range sta.IO.resolver.GetItemIcons(missingItemIcons.GetItemIcons()) {
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
						inItemsMap[itemId].IconURL = job.IconURL
					}
				}

				return inItemsMap, inHasNewResults, nil
			}(iMap, hasNewResults)
			if err != nil {
				return err
			}

			// optionally persisting
			if hasNewResults {
				if err := sta.IO.databases.ItemsDatabase.PersistItems(iMap); err != nil {
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

	// publishing for intake into live auctions
	aiRequest := AuctionsIntakeRequest{irData}
	err := func() error {
		encodedAiRequest, err := json.Marshal(aiRequest)
		if err != nil {
			return err
		}

		return res.Messenger.Publish(subjects.AuctionsIntake, encodedAiRequest)
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
