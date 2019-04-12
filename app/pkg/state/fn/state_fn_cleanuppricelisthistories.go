package fn

import (
	"log"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah/gameversions"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/twinj/uuid"
)

type CleanupPricelistHistoriesStateConfig struct {
	ProjectId string
}

func NewCleanupPricelistHistoriesState(
	config CleanupPricelistHistoriesStateConfig,
) (CleanupPricelistHistoriesState, error) {
	// establishing an initial state
	sta := CleanupPricelistHistoriesState{
		State: state.NewState(uuid.NewV4(), true),
	}

	var err error
	sta.IO.BusClient, err = bus.NewClient(config.ProjectId, "fn-cleanup-pricelist-histories")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return CleanupPricelistHistoriesState{}, err
	}
	sta.pricelistsCleanupTopic, err = sta.IO.BusClient.FirmTopic(string(subjects.CleanupPricelistHistories))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return CleanupPricelistHistoriesState{}, err
	}

	storeClient, err := store.NewClient(config.ProjectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return CleanupPricelistHistoriesState{}, err
	}

	sta.bootStoreBase = store.NewBootBase(storeClient, "us-central1")
	sta.bootBucket, err = sta.bootStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return CleanupPricelistHistoriesState{}, err
	}

	sta.pricelistHistoriesStoreBase = store.NewPricelistHistoriesBaseV2(storeClient, "us-central1")
	sta.pricelistHistoriesBucket, err = sta.pricelistHistoriesStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return CleanupPricelistHistoriesState{}, err
	}

	sta.realmsBase = store.NewRealmsBase(sta.IO.StoreClient, "us-central1", gameversions.Retail)
	sta.realmsBucket, err = sta.realmsBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return CleanupPricelistHistoriesState{}, err
	}

	// establishing bus-listeners
	sta.BusListeners = state.NewBusListeners(state.SubjectBusListeners{
		subjects.CleanupAllPricelistHistories: sta.ListenForCleanupAllPricelistHistories,
	})

	return sta, nil
}

type CleanupPricelistHistoriesState struct {
	state.State

	pricelistsCleanupTopic *pubsub.Topic

	pricelistHistoriesStoreBase store.PricelistHistoriesBaseV2
	pricelistHistoriesBucket    *storage.BucketHandle

	bootStoreBase store.BootBase
	bootBucket    *storage.BucketHandle
	realmsBase    store.RealmsBase
	realmsBucket  *storage.BucketHandle
}

func (sta CleanupPricelistHistoriesState) ListenForCleanupAllPricelistHistories(onReady chan interface{}, stop chan interface{}, onStopped chan interface{}) {
	// establishing subscriber config
	config := bus.SubscribeConfig{
		Stop: stop,
		Callback: func(_ bus.Message) {
			if err := sta.Run(); err != nil {
				logging.WithField("error", err.Error()).Error("Failed to run")
			}
		},
		OnReady:   onReady,
		OnStopped: onStopped,
	}

	// starting up worker for the subscription
	go func() {
		if err := sta.IO.BusClient.SubscribeToTopic(string(subjects.CleanupAllPricelistHistories), config); err != nil {
			logging.WithField("error", err.Error()).Fatal("Failed to subscribe to topic")
		}
	}()
}
