package state

import (
	"encoding/json"
	nats "github.com/nats-io/go-nats"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"time"
)

func newAuctionsIntakeRequest(payload []byte) (AuctionsIntakeRequest, error) {
	ar := &AuctionsIntakeRequest{}
	err := json.Unmarshal(payload, &ar)
	if err != nil {
		return AuctionsIntakeRequest{}, err
	}

	return *ar, nil
}

type RealmMapValue struct {
	Realm        sotah.Realm
	LastModified time.Time
}

type RealmMap struct {
	Values map[blizzard.RealmSlug]RealmMapValue
}

func (rMap RealmMap) toRealms() sotah.Realms {
	out := sotah.Realms{}
	for _, rValue := range rMap.Values {
		out = append(out, rValue.Realm)
	}

	return out
}

type RegionRealmMap = map[blizzard.RegionName]RealmMap

type IntakeRequestData = map[blizzard.RegionName]map[blizzard.RealmSlug]int64

type AuctionsIntakeRequest struct {
	RegionRealmTimestamps IntakeRequestData `json:"region_realm_timestamps"`
}

func (aiRequest AuctionsIntakeRequest) resolve(sta State) (RegionRealmMap, RegionRealmMap, error) {
	includedRegionRealms := RegionRealmMap{}
	excludedRegionRealms := RegionRealmMap{}
	for _, reg := range sta.Regions {
		includedRegionRealms[reg.Name] = RealmMap{map[blizzard.RealmSlug]RealmMapValue{}}

		excludedRegionRealms[reg.Name] = RealmMap{map[blizzard.RealmSlug]RealmMapValue{}}
		for _, rea := range sta.Statuses[reg.Name].Realms {
			excludedRegionRealms[reg.Name].Values[rea.Slug] = RealmMapValue{rea, time.Time{}}
		}
	}

	for rName, realmSlugs := range aiRequest.RegionRealmTimestamps {
		for realmSlug, unixTimestamp := range realmSlugs {
			for _, rea := range sta.Statuses[rName].Realms {
				if rea.Slug != realmSlug {
					continue
				}

				includedRegionRealms[rName].Values[realmSlug] = RealmMapValue{rea, time.Unix(unixTimestamp, 0)}
				delete(excludedRegionRealms[rName].Values, realmSlug)
			}
		}
	}

	return includedRegionRealms, excludedRegionRealms, nil
}

func (aiRequest AuctionsIntakeRequest) handle(sta State) {
	// resolving included and excluded region realms
	includedRegionRealms, excludedRegionRealms, err := aiRequest.resolve(sta)
	if err != nil {
		logging.WithField("error", err.Error()).Info("Failed to resolve auctions-intake-request")

		return
	}

	// misc for metrics
	totalRealms := 0
	for _, status := range sta.Statuses {
		totalRealms += len(status.Realms)
	}
	includedRealmCount := 0
	for _, reas := range includedRegionRealms {
		includedRealmCount += len(reas.Values)
	}
	excludedRealmCount := 0
	for _, reas := range excludedRegionRealms {
		excludedRealmCount += len(reas.Values)
	}

	logging.WithFields(logrus.Fields{
		"included_realms": includedRealmCount,
		"excluded_realms": excludedRealmCount,
		"total_realms":    totalRealms,
	}).Info("Handling auctions-intake-request")

	// misc
	startTime := time.Now()

	// loading in auctions from region-realms
	for rName, rMap := range includedRegionRealms {
		logging.WithFields(logrus.Fields{
			"region": rName,
			"realms": len(rMap.Values),
		}).Debug("Going over realms to load auctions")

		// loading auctions from file cache or gcloud store
		loadedAuctions := func() chan internal.LoadAuctionsJob {
			if sta.UseGCloud {
				return sta.IO.store.LoadRegionRealmMap(rMap)
			}

			return rMap.toRealms().LoadAuctionsFromCacheDir(sta.Resolver.Config)
		}()
		done := sta.PricelistHistoryDatabases.Load(loadedAuctions, *sta.Resolver.Config, sta.Resolver.Store)
		<-done

		logging.WithFields(logrus.Fields{
			"region": rName,
			"realms": len(rMap.Values),
		}).Debug("Finished loading auctions")
	}

	metric.ReportDuration(
		metric.PricelistsIntakeDuration,
		metric.DurationMetrics{
			Duration:       time.Now().Sub(startTime),
			TotalRealms:    totalRealms,
			IncludedRealms: includedRealmCount,
			ExcludedRealms: excludedRealmCount,
		},
		logrus.Fields{},
	)
}

func (sta State) ListenForAuctionsIntake(stop messenger.ListenStopChan) error {
	// spinning up a worker for handling auctions-intake requests
	in := make(chan AuctionsIntakeRequest, 10)
	go func() {
		for {
			aiRequest := <-in

			// misc
			startTime := time.Now()

			includedRegionRealms, excludedRegionRealms, err := aiRequest.resolve(sta)
			if err != nil {
				logging.WithField("error", err.Error()).Error("Failed to resolve auctions-intake-request")

				continue
			}

			totalRealms := 0
			for rName, reas := range sta.Statuses {
				totalRealms += len(reas.Realms.FilterWithWhitelist(sta.Resolver.Config.Whitelist[rName]))
			}
			includedRealmCount := 0
			for _, reas := range includedRegionRealms {
				includedRealmCount += len(reas.Values)
			}
			excludedRealmCount := 0
			for _, reas := range excludedRegionRealms {
				excludedRealmCount += len(reas.Values)
			}

			logging.WithFields(logrus.Fields{
				"included_realms": includedRealmCount,
				"excluded_realms": excludedRealmCount,
				"total_realms":    totalRealms,
			}).Info("Handling auctions-intake-request")

			// metrics
			totalPreviousAuctions := 0
			totalRemovedAuctions := 0
			totalNewAuctions := 0
			totalAuctions := 0
			totalOwners := 0
			currentItemIds := map[blizzard.ItemID]struct{}{}

			// gathering the total number of auctions pre-intake
			logging.Info("Going over all auctions to for pre-intake metrics")
			for statsJob := range sta.LiveAuctionsDatabases.GetStats(nil) {
				if statsJob.Err != nil {
					logging.WithFields(logrus.Fields{
						"error":  statsJob.Err.Error(),
						"region": statsJob.Realm.Region.Name,
						"Realm":  statsJob.Realm.Slug,
					}).Error("Failed to fetch stats from live-auctions database")

					continue
				}

				totalPreviousAuctions += statsJob.Stats.TotalAuctions
			}
			for statsJob := range sta.LiveAuctionsDatabases.GetStats(excludedRegionRealms) {
				if statsJob.Err != nil {
					logging.WithFields(logrus.Fields{
						"error":  statsJob.Err.Error(),
						"region": statsJob.Realm.Region.Name,
						"Realm":  statsJob.Realm.Slug,
					}).Error("Failed to fetch stats from live-auctions database")

					continue
				}

				totalAuctions += statsJob.Stats.TotalAuctions
				totalOwners += len(statsJob.Stats.OwnerNames)
				for _, ID := range statsJob.Stats.ItemIds {
					currentItemIds[ID] = struct{}{}
				}
			}

			// going over auctions in the filecache
			for rName, rMap := range includedRegionRealms {
				logging.WithFields(logrus.Fields{
					"region": rName,
					"realms": len(rMap.Values),
				}).Debug("Going over realms")

				// loading auctions
				loadedAuctions := func() chan internal.LoadAuctionsJob {
					if sta.Resolver.Config.UseGCloud {
						return sta.Resolver.Store.LoadRegionRealmMap(rMap)
					}

					return rMap.toRealms().LoadAuctionsFromCacheDir(sta.Resolver.Config)
				}()
				loadedAuctionsResults := sta.LiveAuctionsDatabases.Load(loadedAuctions)
				for result := range loadedAuctionsResults {
					totalAuctions += len(result.Stats.AuctionIds)
					totalOwners += len(result.Stats.OwnerNames)
					totalRemovedAuctions += result.TotalRemovedAuctions
					totalNewAuctions += result.TotalNewAuctions
				}
				logging.WithFields(logrus.Fields{
					"region": rName,
					"realms": len(rMap.Values),
				}).Debug("Finished loading auctions")
			}

			duration := time.Now().Sub(startTime)

			metric.ReportDuration(
				metric.AuctionsIntakeDuration,
				metric.DurationMetrics{
					Duration:       duration,
					TotalRealms:    totalRealms,
					IncludedRealms: includedRealmCount,
					ExcludedRealms: excludedRealmCount,
				},
				logrus.Fields{
					"total_auctions":          totalAuctions,
					"total_previous_auctions": totalPreviousAuctions,
					"total_new_auctions":      totalNewAuctions,
					"total_removed_auctions":  totalRemovedAuctions,
					"current_owner_count":     totalOwners,
					"current_item_count":      len(currentItemIds),
				},
			)
			logging.Info("Processed all realms")

			encodedAiRequest, err := json.Marshal(aiRequest)
			if err != nil {
				logging.WithField("error", err.Error()).Error("Failed to marshal auctions-intake-request")
			} else {
				sta.Resolver.Messenger.Publish(subjects.PricelistsIntake, encodedAiRequest)
			}
		}
	}()

	// starting up a listener for auctions-intake
	err := sta.IO.messenger.Subscribe(subjects.AuctionsIntake, stop, func(natsMsg nats.Msg) {
		// resolving the request
		aiRequest, err := newAuctionsIntakeRequest(natsMsg.Data)
		if err != nil {
			logging.WithField("error", err.Error()).Error("Failed to parse auctions-intake-request")

			return
		}

		metric.ReportIntakeBufferSize(metric.LiveAuctionsIntake, len(in))
		logging.Info("Received auctions-intake-request")

		in <- aiRequest
	})
	if err != nil {
		return err
	}

	return nil
}
