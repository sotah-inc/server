package main

import (
	"cloud.google.com/go/storage"
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
			err := sta.databases[job.realm.region.Name].persistPricelists(
				job.realm,
				job.lastModified,
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

	// spinning up a worker for handling pricelists-intake requests
	in := make(chan auctionsIntakeRequest, 10)
	go func() {
		for {
			aiRequest := <-in
			aiRequest.handle(sta, loadIn)
		}
	}()

	err := sta.messenger.subscribe(subjects.PricelistsIntake, stop, func(natsMsg nats.Msg) {
		// resolving the request
		aiRequest, err := newAuctionsIntakeRequest(natsMsg.Data)
		if err != nil {
			logging.WithField("error", err.Error()).Error("Failed to parse auctions-intake-request")

			return
		}

		logging.WithFields(logrus.Fields{"intake_buffer_size": len(in)}).Info("Received auctions-intake-request")
		sta.messenger.publishMetric(telegrafMetrics{"intake_buffer_size": int64(len(in))})

		in <- aiRequest
	})
	if err != nil {
		return err
	}

	return nil
}
