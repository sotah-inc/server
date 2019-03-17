package state

import (
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/twinj/uuid"
)

type ProdLiveAuctionsStateConfig struct {
	GCloudProjectID string

	MessengerHost string
	MessengerPort int
}

func NewProdLiveAuctionsState(config ProdLiveAuctionsStateConfig) (ProdLiveAuctionsState, error) {
	// establishing an initial state
	liveAuctionsState := ProdLiveAuctionsState{
		State: NewState(uuid.NewV4(), true),
	}

	// connecting to the messenger host
	mess, err := messenger.NewMessenger(config.MessengerHost, config.MessengerPort)
	if err != nil {
		return ProdLiveAuctionsState{}, err
	}
	liveAuctionsState.IO.Messenger = mess

	// establishing a bus
	logging.Info("Connecting bus-client")
	busClient, err := bus.NewClient(config.GCloudProjectID, "prod-liveauctions")
	if err != nil {
		return ProdLiveAuctionsState{}, err
	}
	liveAuctionsState.IO.BusClient = busClient

	// initializing a reporter
	liveAuctionsState.IO.Reporter = metric.NewReporter(mess)

	// establishing bus-listeners
	liveAuctionsState.BusListeners = NewBusListeners(SubjectBusListeners{
		subjects.ReceiveComputedLiveAuctions: liveAuctionsState.ListenForComputedLiveAuctions,
	})

	return liveAuctionsState, nil
}

type ProdLiveAuctionsState struct {
	State
}

func (metricsState ProdLiveAuctionsState) ListenForComputedLiveAuctions(onReady chan interface{}, stop chan interface{}, onStopped chan interface{}) {
	// establishing subscriber config
	config := bus.SubscribeConfig{
		Stop: stop,
		Callback: func(busMsg bus.Message) {
			logging.WithField("data", len(busMsg.Data)).Info("Received message")

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
