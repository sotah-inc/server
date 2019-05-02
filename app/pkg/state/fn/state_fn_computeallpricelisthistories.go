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

type ComputeAllPricelistHistoriesStateConfig struct {
	ProjectId string

	MessengerHost string
	MessengerPort int
}

func NewComputeAllPricelistHistoriesState(config ComputeAllPricelistHistoriesStateConfig) (ComputeAllPricelistHistoriesState, error) {
	// establishing an initial state
	sta := ComputeAllPricelistHistoriesState{
		State: state.NewState(uuid.NewV4(), true),
	}

	var err error
	sta.IO.BusClient, err = bus.NewClient(config.ProjectId, "fn-compute-all-pricelist-histories")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return ComputeAllPricelistHistoriesState{}, err
	}
	sta.computePricelistHistoriesTopic, err = sta.IO.BusClient.FirmTopic(string(subjects.ComputePricelistHistories))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return ComputeAllPricelistHistoriesState{}, err
	}
	sta.receiveComputedPricelistHistoriesTopic, err = sta.IO.BusClient.FirmTopic(
		string(subjects.ReceiveComputedPricelistHistories),
	)
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return ComputeAllPricelistHistoriesState{}, err
	}

	sta.IO.StoreClient, err = store.NewClient(config.ProjectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return ComputeAllPricelistHistoriesState{}, err
	}

	// establishing bus-listeners
	sta.BusListeners = state.NewBusListeners(state.SubjectBusListeners{
		subjects.ComputeAllPricelistHistories: sta.ListenForComputeAllPricelistHistories,
	})

	return sta, nil
}

type ComputeAllPricelistHistoriesState struct {
	state.State

	computePricelistHistoriesTopic         *pubsub.Topic
	receiveComputedPricelistHistoriesTopic *pubsub.Topic
}

func (sta ComputeAllPricelistHistoriesState) ListenForComputeAllPricelistHistories(onReady chan interface{}, stop chan interface{}, onStopped chan interface{}) {
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
		if err := sta.IO.BusClient.SubscribeToTopic(string(subjects.ComputeAllPricelistHistories), config); err != nil {
			logging.WithField("error", err.Error()).Fatal("Failed to subscribe to topic")
		}
	}()
}
