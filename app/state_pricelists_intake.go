package main

import (
	"time"

	"cloud.google.com/go/storage"
	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/logging"
	"github.com/ihsw/sotah-server/app/subjects"
	"github.com/ihsw/sotah-server/app/util"
	nats "github.com/nats-io/go-nats"
	"github.com/sirupsen/logrus"
)

func (sta state) listenForPricelistsIntake(stop listenStopChan) error {
	// spinning up the workers for persisting realm prices
	loadIn := make(chan loadAuctionsJob)
	worker := func() {
		for job := range loadIn {
			if job.err != nil {
				logging.WithFields(logrus.Fields{
					"error":  job.err.Error(),
					"region": job.realm.region.Name,
					"realm":  job.realm.Slug,
				}).Error("Erroneous job was passed into pricelist intake channel")

				continue
			}

			mAuctions := newMiniAuctionListFromBlizzardAuctions(job.auctions.Auctions)
			err := sta.databases[job.realm.region.Name][job.realm.Slug][job.lastModified.Unix()].persistPricelists(
				newPriceList(mAuctions.itemIds(), mAuctions),
			)
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error":  err.Error(),
					"region": job.realm.region.Name,
					"realm":  job.realm.Slug,
				}).Error("Failed to persist auctions to database")

				continue
			}

			// optionally setting the obj state metadata to processed
			if sta.resolver.config.UseGCloudStorage {
				sto := sta.resolver.store

				bkt := sto.getRealmAuctionsBucket(job.realm)
				obj := bkt.Object(sto.getRealmAuctionsObjectName(job.lastModified))
				objAttrs, err := obj.Attrs(sto.context)
				if err != nil {
					logging.WithFields(logrus.Fields{
						"error":         err.Error(),
						"region":        job.realm.region.Name,
						"realm":         job.realm.Slug,
						"last-modified": job.lastModified.Unix(),
					}).Error("Failed to fetch obj attrs")

					continue
				}

				objMeta := func() map[string]string {
					if objAttrs.Metadata == nil {
						return map[string]string{}
					}

					return objAttrs.Metadata
				}()
				objMeta["state"] = "processed"
				if _, err := obj.Update(sto.context, storage.ObjectAttrsToUpdate{Metadata: objMeta}); err != nil {
					logging.WithFields(logrus.Fields{
						"error":         err.Error(),
						"region":        job.realm.region.Name,
						"realm":         job.realm.Slug,
						"last-modified": job.lastModified.Unix(),
					}).Error("Failed to update metadata of object")

					continue
				}
			}
		}
	}
	postWork := func() {
		return
	}
	util.Work(4, worker, postWork)

	// declaring a channel for queueing up pricelist-intake requests
	listenerIn := make(chan auctionsIntakeRequest, 10)

	// optionally spinning up a collector for producing pricelist-intake requests
	collectorIn := make(chan auctionsIntakeRequest)
	if sta.resolver.config.UseGCloudStorage {
		go func() {
			logging.Info("Starting auctions-intake collector")

			for {
				hasResults := false
				aiRequest := auctionsIntakeRequest{RegionRealmTimestamps: intakeRequestData{}}
				for _, reg := range sta.regions {
					aiRequest.RegionRealmTimestamps[reg.Name] = map[blizzard.RealmSlug]int64{}

					for _, rea := range sta.statuses[reg.Name].Realms {
						// validating taht the realm-auctions bucket exists
						exists, err := sta.resolver.store.realmAuctionsBucketExists(rea)
						if err != nil {
							logging.WithFields(logrus.Fields{
								"error":  err.Error(),
								"region": reg.Name,
								"realm":  rea.Slug,
							}).Error("Failed to check if realm-auctions bucket exists")

							continue
						}
						if exists == false {
							continue
						}

						logging.WithFields(logrus.Fields{
							"region": reg.Name,
							"realm":  rea.Slug,
						}).Debug("Checking store for realm-auctions-object for processing")

						// checking the store for the latest realm-auctions object for processing
						bkt := sta.resolver.store.getRealmAuctionsBucket(rea)
						obj, targetTime, err := sta.resolver.store.getLatestRealmAuctionsObjectForProcessing(bkt)
						if err != nil {
							logging.WithFields(logrus.Fields{
								"error":  err.Error(),
								"region": reg.Name,
								"realm":  rea.Slug,
							}).Error("Failed to fetch latest realm-auctions object for processing")

							continue
						}

						// optionally halting on no results returned
						if targetTime.IsZero() {
							logging.WithFields(logrus.Fields{
								"region": reg.Name,
								"realm":  rea.Slug,
							}).Debug("No results found for processing via auctions-intake collector")

							continue
						}

						// gathering obj attrs for updating metadata
						objAttrs, err := obj.Attrs(sta.resolver.store.context)
						if err != nil {
							logging.WithFields(logrus.Fields{
								"error":  err.Error(),
								"region": reg.Name,
								"realm":  rea.Slug,
							}).Error("Failed to gathering obj attrs")

							continue
						}

						hasResults = true
						aiRequest.RegionRealmTimestamps[reg.Name][rea.Slug] = targetTime.Unix()

						objMeta := func() map[string]string {
							if objAttrs.Metadata == nil {
								return map[string]string{}
							}

							return objAttrs.Metadata
						}()
						objMeta["state"] = "queued"
						if _, err := obj.Update(sta.resolver.store.context, storage.ObjectAttrsToUpdate{Metadata: objMeta}); err != nil {
							logging.WithFields(logrus.Fields{
								"error":         err.Error(),
								"region":        reg.Name,
								"realm":         rea.Slug,
								"last-modified": targetTime.Unix(),
							}).Error("Failed to update metadata of object")

							continue
						}
					}
				}

				if hasResults == false {
					logging.Info("Breaking due to no realm-auctions results found")

					break
				}

				logging.Info("Queueing auctions-intake request into collector channel")
				collectorIn <- aiRequest

				logging.Info("Sleeping for 5s before next pricelist-intake collector loop")
				time.Sleep(5 * time.Second)
			}
		}()
	}

	// spinning up a worker for handling pricelists-intake requests
	go func() {
		for {
			select {
			case aiRequest := <-listenerIn:
				logging.Info("Queueing up auctions-intake-request from the listener")

				collectorIn <- aiRequest
			case aiRequest := <-collectorIn:
				logging.Info("Handling auctions-intake-request from the collector")

				aiRequest.handle(sta, loadIn)
			}
		}
	}()

	err := sta.messenger.subscribe(subjects.PricelistsIntake, stop, func(natsMsg nats.Msg) {
		// resolving the request
		aiRequest, err := newAuctionsIntakeRequest(natsMsg.Data)
		if err != nil {
			logging.WithField("error", err.Error()).Error("Failed to parse auctions-intake-request")

			return
		}

		logging.WithFields(logrus.Fields{"pricelists_intake_buffer_size": len(listenerIn)}).Info("Received auctions-intake-request")
		sta.messenger.publishMetric(telegrafMetrics{"pricelists_intake_buffer_size": int64(len(listenerIn))})

		listenerIn <- aiRequest
	})
	if err != nil {
		return err
	}

	return nil
}
