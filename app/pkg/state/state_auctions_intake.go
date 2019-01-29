package state

import (
	"encoding/json"
	nats "github.com/nats-io/go-nats"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/store"
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

type AuctionsIntakeRequest struct {
	RegionRealmTimestamps RegionRealmTimestamps `json:"region_realm_timestamps"`
}

func (aiRequest AuctionsIntakeRequest) resolve(sta State) (store.RegionRealmTimes, store.RegionRealmTimes, error) {
	includedRegionRealmTimes := store.RegionRealmTimes{}
	excludedRegionRealmTimes := store.RegionRealmTimes{}
	for _, reg := range sta.Regions {
		includedRegionRealmTimes[reg.Name] = store.RealmTimes{}

		excludedRegionRealmTimes[reg.Name] = store.RealmTimes{}
		for _, rea := range sta.Statuses[reg.Name].Realms {
			excludedRegionRealmTimes[reg.Name][rea.Slug] = store.GetAuctionsFromTimesInJob{
				Realm:      rea,
				TargetTime: time.Unix(0, 0),
			}
		}
	}

	for rName, realmSlugs := range aiRequest.RegionRealmTimestamps {
		for realmSlug, unixTimestamp := range realmSlugs {
			for _, rea := range sta.Statuses[rName].Realms {
				if rea.Slug != realmSlug {
					continue
				}

				includedRegionRealmTimes[rName][realmSlug] = store.GetAuctionsFromTimesInJob{
					Realm:      rea,
					TargetTime: time.Unix(int64(unixTimestamp), 0),
				}
				delete(excludedRegionRealmTimes[rName], realmSlug)
			}
		}
	}

	return includedRegionRealmTimes, excludedRegionRealmTimes, nil
}

func (aiRequest AuctionsIntakeRequest) handle(sta State) {
	// resolving included and excluded region realms
	includedRegionRealmTimes, excludedRegionRealmTimes, err := aiRequest.resolve(sta)
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
	for _, reas := range includedRegionRealmTimes {
		includedRealmCount += len(reas)
	}
	excludedRealmCount := 0
	for _, reas := range excludedRegionRealmTimes {
		excludedRealmCount += len(reas)
	}

	logging.WithFields(logrus.Fields{
		"included_realms": includedRealmCount,
		"excluded_realms": excludedRealmCount,
		"total_realms":    totalRealms,
	}).Info("Handling auctions-intake-request")

	// misc
	startTime := time.Now()

	// loading in auctions from region-realms
	for regionName, realmTimes := range includedRegionRealmTimes {
		logging.WithFields(logrus.Fields{
			"region": regionName,
			"realms": len(realmTimes),
		}).Debug("Going over realms to load auctions")

		// loading auctions from file cache or gcloud store
		loadedAuctions := func() chan database.LoadInJob {
			out := make(chan database.LoadInJob)

			go func() {
				if sta.UseGCloud {
					for outJob := range sta.IO.store.GetAuctionsFromTimes(realmTimes) {
						if outJob.Err != nil {
							logrus.WithFields(outJob.ToLogrusFields()).Error("Failed to receive auctions from gcloud store")

							continue
						}

						out <- database.LoadInJob{
							Realm:      outJob.Realm,
							TargetTime: outJob.TargetTime,
							Auctions:   outJob.Auctions,
						}
					}

					return
				}

				for outJob := range sta.IO.diskStore.GetAuctionsByRealms(store.RealmTimesToRealms(realmTimes)) {
					if outJob.Err != nil {
						logrus.WithFields(outJob.ToLogrusFields()).Error("Failed to receive auctions from disk-store")

						continue
					}

					out <- database.LoadInJob{
						Realm:      outJob.Realm,
						TargetTime: outJob.LastModified,
						Auctions:   outJob.Auctions,
					}
				}
			}()

			return out
		}()
		done := sta.IO.databases.PricelistHistoryDatabases.Load(loadedAuctions)
		<-done

		logging.WithFields(logrus.Fields{
			"region": regionName,
			"realms": len(realmTimes),
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
