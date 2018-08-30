package main

import (
	"encoding/json"
	"errors"
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

func (aiRequest auctionsIntakeRequest) resolve(sta state) (map[regionName]realms, error) {
	out := map[regionName]realms{}
	for rNAme, realmSlugs := range aiRequest.RegionRealmSlugs {
		statusValue, ok := sta.statuses[rNAme]
		if !ok {
			return nil, errors.New("Invalid region")
		}

		out[rNAme] = realms{}
		for _, rea := range statusValue.Realms {
			for _, inRealm := range realmSlugs {
				if rea.Slug != inRealm {
					continue
				}

				out[rNAme] = append(out[rNAme], rea)
			}
		}
	}

	return out, nil
}

type auctionsIntakeRequest struct {
	RegionRealmSlugs map[regionName][]blizzard.RealmSlug `json:"region_realmslugs"`
}

func (sta state) listenForAuctionsIntake(stop listenStopChan) error {
	// spinning up a worker for handling auctions-intake requests
	in := make(chan auctionsIntakeRequest, 10)
	go func() {
		for aiRequest := range in {
			regionRealms, err := aiRequest.resolve(sta)
			if err != nil {
				log.WithField("error", err.Error()).Info("Failed to resolve auctions-intake-request")

				continue
			}

			totalRealms := 0
			for rName, reas := range sta.statuses {
				totalRealms += len(reas.Realms.filterWithWhitelist(sta.resolver.config.Whitelist[rName]))
			}
			processedRealms := 0
			for _, reas := range regionRealms {
				processedRealms += len(reas)
			}

			log.WithFields(log.Fields{
				"processed_realms": processedRealms,
				"total_realms":     totalRealms,
			}).Info("Handling auctions-intake-request")

			// misc
			startTime := time.Now()

			// metrics
			currentItemIds := map[blizzard.ItemID]struct{}{}
			totalPreviousAuctions := 0
			totalRemovedAuctions := 0
			totalNewAuctions := 0
			totalOwners := 0
			totalAuctions := 0

			// going over auctions in the filecache
			for rName, reas := range regionRealms {
				// gathering the total number of auctions pre-collection
				for _, rea := range reas {
					for _, auc := range sta.auctions[rName][rea.Slug] {
						totalPreviousAuctions += len(auc.AucList)
					}
				}

				// loading auctions from file cache
				loadedAuctions := reas.loadAuctions(sta.resolver.config, sta.resolver.store)
				for job := range loadedAuctions {
					if job.err != nil {
						log.WithFields(log.Fields{
							"region": job.realm.region.Name,
							"realm":  job.realm.Slug,
							"error":  err.Error(),
						}).Info("Failed to load auctions from filecache")

						continue
					}

					if true {
						continue
					}

					// gathering previous and new auction ids for comparison
					removedAuctionIds := map[int64]struct{}{}
					for _, mAuction := range sta.auctions[job.realm.region.Name][job.realm.Slug] {
						for _, auc := range mAuction.AucList {
							removedAuctionIds[auc] = struct{}{}
						}
					}
					newAuctionIds := map[int64]struct{}{}
					for _, auc := range job.auctions.Auctions {
						if _, ok := removedAuctionIds[auc.Auc]; ok {
							delete(removedAuctionIds, auc.Auc)
						}

						newAuctionIds[auc.Auc] = struct{}{}
					}
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

				// going over current auctions for metrics
				for _, rea := range reas {
					for _, auc := range sta.auctions[rName][rea.Slug] {
						// going over new auctions data
						realmOwnerNames := map[ownerName]struct{}{}
						for _, auc := range sta.auctions[rName][rea.Slug] {
							realmOwnerNames[ownerName(auc.Owner)] = struct{}{}
							currentItemIds[auc.ItemID] = struct{}{}
						}
						totalAuctions += len(auc.AucList)
						totalOwners += len(realmOwnerNames)
					}
				}
			}

			log.WithFields(log.Fields{
				"total_realms":     totalRealms,
				"processed_realms": processedRealms,
			}).Info("Processed all realms")
			sta.messenger.publishMetric(telegrafMetrics{
				"intake_duration":        int64(time.Now().Unix() - startTime.Unix()),
				"intake_count":           int64(processedRealms),
				"total_auctions":         int64(totalAuctions),
				"total_new_auctions":     int64(totalNewAuctions),
				"total_removed_auctions": int64(totalRemovedAuctions),
				"current_owner_count":    int64(totalOwners),
				"current_item_count":     int64(len(currentItemIds)),
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

		log.WithField("intake_buffer_size", len(in)).Info("Received auctions-intake-request")
		sta.messenger.publishMetric(telegrafMetrics{"intake_buffer_size": int64(len(in))})

		in <- aiRequest
	})
	if err != nil {
		return err
	}

	return nil
}
