package state

import (
	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
	"github.com/sotah-inc/server/app/pkg/metric"
)

type liveAuctionsIntakeRequest struct {
}

func (sta State) ListenForLiveAuctionsIntake(stop messenger.ListenStopChan) error {
	// starting up a listener for live-auctions-intake
	err := sta.IO.messenger.Subscribe(subjects.LiveAuctionsIntake, stop, func(natsMsg nats.Msg) {
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
}
