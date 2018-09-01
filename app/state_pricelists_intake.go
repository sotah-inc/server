package main

import (
	"time"

	"github.com/ihsw/sotah-server/app/subjects"
	nats "github.com/nats-io/go-nats"
	log "github.com/sirupsen/logrus"
)

func (sta state) listenForPricelistsIntake(stop listenStopChan) error {
	// spinning up a worker for handling pricelists-intake requests
	in := make(chan auctionsIntakeRequest, 10)
	go func() {
		for {
			aiRequest := <-in

			includedRegionRealms, _, err := aiRequest.resolve(sta)
			if err != nil {
				log.WithField("error", err.Error()).Info("Failed to resolve auctions-intake-request")

				continue
			}

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

			// going over auctions
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
						}).Info("Failed to load auctions from filecache")

						continue
					}

					mAuctions := newMiniAuctionListFromBlizzardAuctions(job.auctions.Auctions)
					err := sta.databases[job.realm.region.Name][job.realm.Slug].persistPricelists(
						job.lastModified,
						newPriceList(mAuctions.itemIds(), mAuctions),
					)
					if err != nil {
						log.WithFields(log.Fields{
							"region": job.realm.region.Name,
							"realm":  job.realm.Slug,
							"error":  err.Error(),
						}).Info("Failed to persist auctions to database")

						continue
					}
				}
				log.WithFields(log.Fields{
					"region": rName,
					"realms": len(rMap.values),
				}).Info("Finished loading auctions")
			}

			log.WithFields(log.Fields{"included_realms": includedRealmCount}).Info("Processed all realms")
			sta.messenger.publishMetric(telegrafMetrics{
				"pricelists_intake_duration": int64(time.Now().Unix() - startTime.Unix()),
			})
		}
	}()

	err := sta.messenger.subscribe(subjects.PricelistsIntake, stop, func(natsMsg nats.Msg) {
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
