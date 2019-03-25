package state

import (
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/sotah-inc/server/app/pkg/util"
	"github.com/twinj/uuid"
)

type ProdItemsStateConfig struct {
	GCloudProjectID string

	MessengerHost string
	MessengerPort int

	ItemsDatabaseDir string
}

func NewProdItemsState(config ProdItemsStateConfig) (ProdItemsState, error) {
	// establishing an initial state
	itemsState := ProdItemsState{
		State: NewState(uuid.NewV4(), true),
	}

	// connecting to the messenger host
	mess, err := messenger.NewMessenger(config.MessengerHost, config.MessengerPort)
	if err != nil {
		return ProdItemsState{}, err
	}
	itemsState.IO.Messenger = mess

	// establishing a bus
	logging.Info("Connecting bus-client")
	busClient, err := bus.NewClient(config.GCloudProjectID, "prod-items")
	if err != nil {
		return ProdItemsState{}, err
	}
	itemsState.IO.BusClient = busClient

	// establishing a store
	storeClient, err := store.NewClient(config.GCloudProjectID)
	if err != nil {
		return ProdItemsState{}, err
	}
	itemsState.IO.StoreClient = storeClient

	itemsState.ItemsBase = store.NewItemsBase(storeClient, "us-central1")
	itemsState.ItemsBucket, err = itemsState.ItemsBase.GetFirmBucket()
	if err != nil {
		return ProdItemsState{}, err
	}

	// ensuring database paths exist
	databasePaths := []string{fmt.Sprintf("%s/items", config.ItemsDatabaseDir)}
	if err := util.EnsureDirsExist(databasePaths); err != nil {
		return ProdItemsState{}, err
	}

	// initializing a reporter
	itemsState.IO.Reporter = metric.NewReporter(mess)

	// loading the items database
	logging.Info("Connecting to items database")
	iBase, err := database.NewItemsDatabase(config.ItemsDatabaseDir)
	if err != nil {
		return ProdItemsState{}, err
	}
	itemsState.IO.Databases.ItemsDatabase = iBase

	// establishing bus-listeners
	itemsState.BusListeners = NewBusListeners(SubjectBusListeners{})

	// establishing messenger-listeners
	itemsState.Listeners = NewListeners(SubjectListeners{})

	return itemsState, nil
}

type ProdItemsState struct {
	State

	ItemsBase   store.ItemsBase
	ItemsBucket *storage.BucketHandle
}
