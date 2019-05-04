package fn

import (
	"log"

	"cloud.google.com/go/pubsub"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/twinj/uuid"
)

type ComputeAllLiveAuctionsStateConfig struct {
	ProjectId string
}

func NewComputeAllLiveAuctionsState(config ComputeAllLiveAuctionsStateConfig) (ComputeAllLiveAuctionsState, error) {
	// establishing an initial state
	sta := ComputeAllLiveAuctionsState{
		State: state.NewState(uuid.NewV4(), true),
	}

	var err error
	sta.IO.BusClient, err = bus.NewClient(config.ProjectId, "fn-compute-all-live-auctions")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return ComputeAllLiveAuctionsState{}, err
	}
	sta.computeLiveAuctionsTopic, err = sta.IO.BusClient.FirmTopic(string(subjects.ComputeLiveAuctions))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return ComputeAllLiveAuctionsState{}, err
	}
	sta.syncAllItemsTopic, err = sta.IO.BusClient.FirmTopic(string(subjects.SyncAllItems))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return ComputeAllLiveAuctionsState{}, err
	}
	sta.receiveComputedLiveAuctionsTopic, err = sta.IO.BusClient.FirmTopic(
		string(subjects.ReceiveComputedLiveAuctions),
	)
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return ComputeAllLiveAuctionsState{}, err
	}

	sta.IO.StoreClient, err = store.NewClient(config.ProjectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return ComputeAllLiveAuctionsState{}, err
	}

	// establishing bus-listeners
	sta.BusListeners = state.NewBusListeners(state.SubjectBusListeners{
		subjects.ComputeAllLiveAuctions: sta.ListenForComputeAllLiveAuctions,
	})

	return sta, nil
}

type ComputeAllLiveAuctionsState struct {
	state.State

	computeLiveAuctionsTopic         *pubsub.Topic
	syncAllItemsTopic                *pubsub.Topic
	receiveComputedLiveAuctionsTopic *pubsub.Topic
}

func (sta ComputeAllLiveAuctionsState) ListenForComputeAllLiveAuctions(onReady chan interface{}, stop chan interface{}, onStopped chan interface{}) {
	in := make(chan string)
	go func() {
		for {
			data := <-in
			if err := sta.Run(data); err != nil {
				logging.WithField("error", err.Error()).Error("Failed to run")
			}
		}
	}()

	// establishing subscriber config
	config := bus.SubscribeConfig{
		Stop: stop,
		Callback: func(busMsg bus.Message) {
			in <- busMsg.Data
		},
		OnReady:   onReady,
		OnStopped: onStopped,
	}

	// starting up worker for the subscription
	go func() {
		if err := sta.IO.BusClient.SubscribeToTopic(string(subjects.ComputeAllLiveAuctions), config); err != nil {
			logging.WithField("error", err.Error()).Fatal("Failed to subscribe to topic")
		}
	}()
}
