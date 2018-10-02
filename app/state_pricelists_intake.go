package main

import (
	"github.com/ihsw/sotah-server/app/logging"
	"github.com/ihsw/sotah-server/app/subjects"
	nats "github.com/nats-io/go-nats"
	"github.com/sirupsen/logrus"
)

func (sta state) listenForPricelistsIntake(stop listenStopChan) error {
	// spinning up a loader for handling pricelist-intake requests
	loadIn := sta.pricelistHistoryDatabases.startLoader(*sta.resolver.config, sta.resolver.store)

	// declaring a channel for queueing up pricelist-intake requests from the listener
	listenerIn := make(chan auctionsIntakeRequest, 10)

	// spinning up a worker for pricelist-intake requests
	go func() {
		for {
			aiRequest := <-listenerIn
			logging.Info("Handling auctions-intake-request from the listener")

			aiRequest.handle(sta, loadIn)

			logging.Info("Finished handling auctions-intake-request from the listener")
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
