package state

import (
	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
	"github.com/sotah-inc/server/app/pkg/metric"
)

func (sta State) ListenForPricelistsIntake(stop messenger.ListenStopChan) error {
	// declaring a channel for queueing up pricelist-intake requests from the listener
	listenerIn := make(chan AuctionsIntakeRequest, 10)

	// spinning up a worker for pricelist-intake requests
	go func() {
		for {
			aiRequest := <-listenerIn
			logging.Info("Handling auctions-intake-request from the listener")

			aiRequest.handle(sta)

			logging.Info("Finished handling auctions-intake-request from the listener")
		}
	}()

	err := sta.IO.messenger.Subscribe(subjects.PricelistsIntake, stop, func(natsMsg nats.Msg) {
		// resolving the request
		aiRequest, err := newAuctionsIntakeRequest(natsMsg.Data)
		if err != nil {
			logging.WithField("error", err.Error()).Error("Failed to parse auctions-intake-request")

			return
		}

		metric.ReportIntakeBufferSize(metric.PricelistsIntake, len(listenerIn))
		logging.Info("Received auctions-intake-request")

		listenerIn <- aiRequest
	})
	if err != nil {
		return err
	}

	return nil
}
