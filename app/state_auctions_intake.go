package main

import (
	"encoding/json"
	"time"

	"github.com/ihsw/sotah-server/app/blizzard"

	"github.com/ihsw/sotah-server/app/subjects"
	nats "github.com/nats-io/go-nats"
	log "github.com/sirupsen/logrus"
)

func newAuctionsIntakeRequest(payload []byte) (auctionsIntakeRequest, error) {
	ar := &auctionsIntakeRequest{}
	err := json.Unmarshal(payload, &ar)
	if err != nil {
		return auctionsIntakeRequest{}, err
	}

	return *ar, nil
}

type realmMapValue struct {
	realm        realm
	lastModified time.Time
}

type realmMap struct {
	values map[blizzard.RealmSlug]realmMapValue
}

func (rMap realmMap) toRealms() realms {
	out := realms{}
	for _, rValue := range rMap.values {
		out = append(out, rValue.realm)
	}

	return out
}

type regionRealmMap = map[regionName]realmMap

func (aiRequest auctionsIntakeRequest) resolve(sta state) (regionRealmMap, regionRealmMap, error) {
	includedRegionRealms := regionRealmMap{}
	excludedRegionRealms := regionRealmMap{}
	for _, reg := range sta.regions {
		wList := sta.resolver.config.Whitelist[reg.Name]
		includedRegionRealms[reg.Name] = realmMap{map[blizzard.RealmSlug]realmMapValue{}}

		excludedRegionRealms[reg.Name] = realmMap{map[blizzard.RealmSlug]realmMapValue{}}
		for _, rea := range sta.statuses[reg.Name].Realms.filterWithWhitelist(wList) {
			excludedRegionRealms[reg.Name].values[rea.Slug] = realmMapValue{rea, time.Time{}}
		}
	}

	for rName, realmSlugs := range aiRequest.RegionRealmTimestamps {
		wList := sta.resolver.config.Whitelist[rName]

		for realmSlug, unixTimestamp := range realmSlugs {
			for _, rea := range sta.statuses[rName].Realms.filterWithWhitelist(wList) {
				if rea.Slug != realmSlug {
					continue
				}

				includedRegionRealms[rName].values[realmSlug] = realmMapValue{rea, time.Unix(unixTimestamp, 0)}
				delete(excludedRegionRealms[rName].values, realmSlug)
			}
		}
	}

	return includedRegionRealms, excludedRegionRealms, nil
}

type intakeRequestData = map[regionName]map[blizzard.RealmSlug]int64

type auctionsIntakeRequest struct {
	RegionRealmTimestamps intakeRequestData `json:"region_realm_timestamps"`
}

func (sta state) listenForAuctionsIntake(stop listenStopChan) error {
	// spinning up a worker for handling auctions-intake requests
	in := make(chan auctionsIntakeRequest, 10)
	go func() {
		for {
			aiRequest := <-in

			includedRegionRealms, excludedRegionRealms, err := aiRequest.resolve(sta)
			if err != nil {
				log.WithField("error", err.Error()).Info("Failed to resolve auctions-intake-request")

				continue
			}

			totalRealms := 0
			for rName, reas := range sta.statuses {
				totalRealms += len(reas.Realms.filterWithWhitelist(sta.resolver.config.Whitelist[rName]))
			}
			includedRealmCount := 0
			for _, reas := range includedRegionRealms {
				includedRealmCount += len(reas.values)
			}
			excludedRealmCount := 0
			for _, reas := range excludedRegionRealms {
				excludedRealmCount += len(reas.values)
			}

			log.WithFields(log.Fields{
				"included_realms": includedRealmCount,
				"excluded_realms": excludedRealmCount,
				"total_realms":    totalRealms,
			}).Info("Handling auctions-intake-request")

			// misc
			startTime := time.Now()

			// metrics
			totalPreviousAuctions := 0
			totalRemovedAuctions := 0
			totalNewAuctions := 0
			totalAuctions := 0
			totalOwners := 0
			currentItemIds := map[blizzard.ItemID]struct{}{}

			// gathering the total number of auctions pre-intake
			log.Info("Going over all auctions to for pre-intake metrics")
			for _, reg := range sta.regions {
				for _, rea := range sta.statuses[reg.Name].Realms {
					for _, auc := range sta.auctions[reg.Name][rea.Slug] {
						totalPreviousAuctions += len(auc.AucList)
					}
				}
			}
			for rName, regionRealms := range excludedRegionRealms {
				for rSlug := range regionRealms.values {

					realmOwnerNames := map[ownerName]struct{}{}
					for _, auc := range sta.auctions[rName][rSlug] {
						totalAuctions += len(auc.AucList)
						realmOwnerNames[ownerName(auc.Owner)] = struct{}{}
						currentItemIds[auc.ItemID] = struct{}{}
					}
					totalOwners += len(realmOwnerNames)
				}
			}

			// going over auctions in the filecache
			for rName, rMap := range includedRegionRealms {
				log.WithFields(log.Fields{
					"region": rName,
					"realms": len(rMap.values),
				}).Info("Going over realms")

				// loading auctions from file cache
				loadedAuctions := func() chan loadAuctionsJob {
					if sta.resolver.config.UseGCloudStorage {
						return sta.resolver.store.loadRegionRealmMap(rMap)
					}

					return rMap.toRealms().loadAuctionsFromCacheDir(sta.resolver.config)
				}()
				for job := range loadedAuctions {
					if job.err != nil {
						log.WithFields(log.Fields{
							"region": job.realm.region.Name,
							"realm":  job.realm.Slug,
							"error":  err.Error(),
						}).Info("Failed to load auctions")

						continue
					}

					// gathering metrics of new auctions
					totalAuctions += len(job.auctions.Auctions)

					// gathering previous and new auction ids for comparison
					removedAuctionIds := map[int64]struct{}{}
					for _, mAuction := range sta.auctions[job.realm.region.Name][job.realm.Slug] {
						for _, auc := range mAuction.AucList {
							removedAuctionIds[auc] = struct{}{}
						}
					}
					realmOwnerNames := map[ownerName]struct{}{}
					newAuctionIds := map[int64]struct{}{}
					for _, auc := range job.auctions.Auctions {
						if _, ok := removedAuctionIds[auc.Auc]; ok {
							delete(removedAuctionIds, auc.Auc)
						}

						newAuctionIds[auc.Auc] = struct{}{}
						realmOwnerNames[ownerName(auc.Owner)] = struct{}{}
						currentItemIds[auc.Item] = struct{}{}
					}
					totalOwners += len(realmOwnerNames)
					for _, mAuction := range sta.auctions[job.realm.region.Name][job.realm.Slug] {
						for _, auc := range mAuction.AucList {
							if _, ok := newAuctionIds[auc]; ok {
								delete(newAuctionIds, auc)
							}
						}
					}
					totalRemovedAuctions += len(removedAuctionIds)
					totalNewAuctions += len(newAuctionIds)

					sta.auctions[job.realm.region.Name][job.realm.Slug] = newMiniAuctionListFromBlizzardAuctions(job.auctions.Auctions)
				}
				log.WithFields(log.Fields{
					"region": rName,
					"realms": len(rMap.values),
				}).Info("Finished loading auctions")
			}

			log.WithFields(log.Fields{
				"total_realms":    totalRealms,
				"included_realms": includedRealmCount,
				"excluded_realms": excludedRealmCount,
			}).Info("Processed all realms")
			sta.messenger.publishMetric(telegrafMetrics{
				"intake_duration":         int64(time.Now().Unix() - startTime.Unix()),
				"intake_count":            int64(includedRealmCount),
				"total_auctions":          int64(totalAuctions),
				"total_previous_auctions": int64(totalPreviousAuctions),
				"total_new_auctions":      int64(totalNewAuctions),
				"total_removed_auctions":  int64(totalRemovedAuctions),
				"current_owner_count":     int64(totalOwners),
				"current_item_count":      int64(len(currentItemIds)),
			})
		}
	}()

	// starting up a listener for auctions-intake
	err := sta.messenger.subscribe(subjects.AuctionsIntake, stop, func(natsMsg nats.Msg) {
		// resolving the request
		aiRequest, err := newAuctionsIntakeRequest(natsMsg.Data)
		if err != nil {
			log.Info("Failed to parse auctions-intake-request")

			return
		}

		log.WithFields(log.Fields{"intake_buffer_size": len(in)}).Info("Received auctions-intake-request")
		sta.messenger.publishMetric(telegrafMetrics{"intake_buffer_size": int64(len(in))})

		in <- aiRequest
	})
	if err != nil {
		return err
	}

	return nil
}
