package fn

import (
	"log"

	"cloud.google.com/go/pubsub"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/twinj/uuid"
)

type SyncAllItemsStateConfig struct {
	ProjectId string
}

func NewSyncAllItemsState(config SyncAllItemsStateConfig) (SyncAllItemsState, error) {
	// establishing an initial state
	sta := SyncAllItemsState{
		State: state.NewState(uuid.NewV4(), true),
	}

	var err error
	sta.IO.BusClient, err = bus.NewClient(config.ProjectId, "fn-sync-all-items")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return SyncAllItemsState{}, err
	}
	sta.syncItemsTopic, err = sta.IO.BusClient.FirmTopic(string(subjects.SyncItems))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return SyncAllItemsState{}, err
	}
	sta.syncItemIconsTopic, err = sta.IO.BusClient.FirmTopic(string(subjects.SyncItemIcons))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return SyncAllItemsState{}, err
	}
	sta.filterInItemsToSyncTopic, err = sta.IO.BusClient.FirmTopic(string(subjects.FilterInItemsToSync))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return SyncAllItemsState{}, err
	}

	// establishing bus-listeners
	sta.BusListeners = state.NewBusListeners(state.SubjectBusListeners{
		subjects.SyncAllItems: sta.ListenForSyncAllItems,
	})

	return sta, nil
}

type SyncAllItemsState struct {
	state.State

	syncItemsTopic           *pubsub.Topic
	syncItemIconsTopic       *pubsub.Topic
	filterInItemsToSyncTopic *pubsub.Topic
}

func (sta SyncAllItemsState) ListenForSyncAllItems(onReady chan interface{}, stop chan interface{}, onStopped chan interface{}) {
	// establishing subscriber config
	config := bus.SubscribeConfig{
		Stop: stop,
		Callback: func(busMsg bus.Message) {
			if err := sta.Run(busMsg); err != nil {
				logging.WithField("error", err.Error()).Error("Failed to run")
			}
		},
		OnReady:   onReady,
		OnStopped: onStopped,
	}

	// starting up worker for the subscription
	go func() {
		if err := sta.IO.BusClient.SubscribeToTopic(string(subjects.SyncAllItems), config); err != nil {
			logging.WithField("error", err.Error()).Fatal("Failed to subscribe to topic")
		}
	}()
}
