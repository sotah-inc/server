package state

import (
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/sotah-inc/server/app/pkg/util"
	"github.com/twinj/uuid"
)

type ProdPricelistHistoriesStateConfig struct {
	GCloudProjectID string

	MessengerHost string
	MessengerPort int

	PricelistHistoriesDatabaseDir string
}

func NewProdPricelistHistoriesState(config ProdPricelistHistoriesStateConfig) (ProdPricelistHistoriesState, error) {
	// establishing an initial state
	phState := ProdPricelistHistoriesState{
		State: NewState(uuid.NewV4(), true),
	}

	// connecting to the messenger host
	mess, err := messenger.NewMessenger(config.MessengerHost, config.MessengerPort)
	if err != nil {
		return ProdPricelistHistoriesState{}, err
	}
	phState.IO.Messenger = mess

	// establishing a bus
	logging.Info("Connecting bus-client")
	busClient, err := bus.NewClient(config.GCloudProjectID, "prod-pricelisthistories")
	if err != nil {
		return ProdPricelistHistoriesState{}, err
	}
	phState.IO.BusClient = busClient

	// establishing a store
	storeClient, err := store.NewClient(config.GCloudProjectID)
	if err != nil {
		return ProdPricelistHistoriesState{}, err
	}
	phState.IO.StoreClient = storeClient

	phState.PricelistHistoriesBase = store.NewPricelistHistoriesBaseV2(storeClient, "us-central1")
	phState.PricelistHistoriesBucket, err = phState.PricelistHistoriesBase.GetFirmBucket()
	if err != nil {
		return ProdPricelistHistoriesState{}, err
	}

	bootBase := store.NewBootBase(storeClient, "us-central1")

	// gathering region-realms
	statuses := sotah.Statuses{}
	bootBucket, err := bootBase.GetFirmBucket()
	if err != nil {
		return ProdPricelistHistoriesState{}, err
	}
	regionRealms, err := bootBase.GetRegionRealms(bootBucket)
	if err != nil {
		return ProdPricelistHistoriesState{}, err
	}
	for regionName, realms := range regionRealms {
		statuses[regionName] = sotah.Status{Realms: realms}
	}
	phState.Statuses = statuses

	// ensuring database paths exist
	databasePaths := []string{}
	for regionName, realms := range regionRealms {
		for _, realm := range realms {
			databasePaths = append(databasePaths, fmt.Sprintf(
				"%s/pricelist-histories/%s/%s",
				config.PricelistHistoriesDatabaseDir,
				regionName,
				realm.Slug,
			))
		}
	}
	if err := util.EnsureDirsExist(databasePaths); err != nil {
		return ProdPricelistHistoriesState{}, err
	}

	// initializing a reporter
	phState.IO.Reporter = metric.NewReporter(mess)

	// loading the pricelist-histories databases
	logging.Info("Connecting to pricelist-histories databases")
	phdBases, err := database.NewPricelistHistoryDatabases(config.PricelistHistoriesDatabaseDir, phState.Statuses)
	if err != nil {
		return ProdPricelistHistoriesState{}, err
	}
	phState.IO.Databases.PricelistHistoryDatabases = phdBases

	// loading the meta database
	logging.Info("Connecting to the meta database")
	metaDatabase, err := database.NewMetaDatabase(config.PricelistHistoriesDatabaseDir)
	if err != nil {
		return ProdPricelistHistoriesState{}, err
	}
	phState.IO.Databases.MetaDatabase = metaDatabase

	// establishing bus-listeners
	phState.BusListeners = NewBusListeners(SubjectBusListeners{
		subjects.ReceiveComputedPricelistHistories: phState.ListenForComputedPricelistHistories,
	})

	return phState, nil
}

type ProdPricelistHistoriesState struct {
	State

	PricelistHistoriesBase   store.PricelistHistoriesBaseV2
	PricelistHistoriesBucket *storage.BucketHandle
}
