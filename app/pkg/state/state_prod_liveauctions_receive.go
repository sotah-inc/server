package state

import (
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func HandleComputedLiveAuctions(metricsState ProdLiveAuctionsState, tuples bus.RegionRealmTimestampTuples) {
	logging.WithField("tuples", len(tuples)).Info("Received tuples")
}

func (metricsState ProdLiveAuctionsState) ListenForComputedLiveAuctions(onReady chan interface{}, stop chan interface{}, onStopped chan interface{}) {
	// establishing subscriber config
	config := bus.SubscribeConfig{
		Stop: stop,
		Callback: func(busMsg bus.Message) {
			tuples, err := bus.NewRegionRealmTimestampTuples(busMsg.Data)
			if err != nil {
				logging.WithField("error", err.Error()).Error("Failed to decode region-realm-timestamps tuples")

				return
			}

			HandleComputedLiveAuctions(metricsState, tuples)

			return
		},
		OnReady:   onReady,
		OnStopped: onStopped,
	}

	// starting up worker for the subscription
	go func() {
		if err := metricsState.IO.BusClient.SubscribeToTopic(string(subjects.ReceiveComputedLiveAuctions), config); err != nil {
			logging.WithField("error", err.Error()).Fatal("Failed to subscribe to topic")
		}
	}()
}
