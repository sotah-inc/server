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

type ProdLiveAuctionsStateConfig struct {
	GCloudProjectID string

	MessengerHost string
	MessengerPort int

	LiveAuctionsDatabaseDir string
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

	// establishing a store
	storeClient, err := store.NewClient(config.GCloudProjectID)
	if err != nil {
		return ProdLiveAuctionsState{}, err
	}
	liveAuctionsState.IO.StoreClient = storeClient

	liveAuctionsState.LiveAuctionsBase = store.NewLiveAuctionsBase(storeClient, "us-central1")
	liveAuctionsState.LiveAuctionsBucket, err = liveAuctionsState.LiveAuctionsBase.GetFirmBucket()
	if err != nil {
		return ProdLiveAuctionsState{}, err
	}

	bootBase := store.NewBootBase(storeClient, "us-central1")

	// gathering region-realms
	statuses := sotah.Statuses{}
	bootBucket, err := bootBase.GetFirmBucket()
	if err != nil {
		return ProdLiveAuctionsState{}, err
	}
	regionRealms, err := bootBase.GetRegionRealms(bootBucket)
	if err != nil {
		return ProdLiveAuctionsState{}, err
	}
	for regionName, realms := range regionRealms {
		statuses[regionName] = sotah.Status{Realms: realms}
	}
	liveAuctionsState.Statuses = statuses

	// ensuring database paths exist
	databasePaths := []string{}
	for regionName, realms := range regionRealms {
		for _, realm := range realms {
			databasePaths = append(databasePaths, fmt.Sprintf(
				"%s/live-auctions/%s/%s",
				config.LiveAuctionsDatabaseDir,
				regionName,
				realm.Slug,
			))
		}
	}
	if err := util.EnsureDirsExist(databasePaths); err != nil {
		return ProdLiveAuctionsState{}, err
	}

	// initializing a reporter
	liveAuctionsState.IO.Reporter = metric.NewReporter(mess)

	// loading the live-auctions databases
	logging.Info("Connecting to live-auctions databases")
	ladBases, err := database.NewLiveAuctionsDatabases(config.LiveAuctionsDatabaseDir, liveAuctionsState.Statuses)
	if err != nil {
		return ProdLiveAuctionsState{}, err
	}
	liveAuctionsState.IO.Databases.LiveAuctionsDatabases = ladBases

	// establishing bus-listeners
	liveAuctionsState.BusListeners = NewBusListeners(SubjectBusListeners{
		subjects.ReceiveComputedLiveAuctions: liveAuctionsState.ListenForComputedLiveAuctions,
	})

	// establishing messenger-listeners
	liveAuctionsState.Listeners = NewListeners(SubjectListeners{
		subjects.Auctions:    liveAuctionsState.ListenForAuctions,
		subjects.OwnersQuery: liveAuctionsState.ListenForOwnersQuery,
		subjects.PriceList:   liveAuctionsState.ListenForPricelist,
	})

	return liveAuctionsState, nil
}

type ProdLiveAuctionsState struct {
	State

	LiveAuctionsBase   store.LiveAuctionsBase
	LiveAuctionsBucket *storage.BucketHandle
}
