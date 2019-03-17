package state

import (
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
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
	stor, err := store.NewClient(config.GCloudProjectID)
	if err != nil {
		return ProdLiveAuctionsState{}, err
	}
	liveAuctionsState.IO.StoreClient = stor
	liveAuctionsState.LiveAuctionsBase = store.NewLiveAuctionsBase(stor, "us-central1")

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

	return liveAuctionsState, nil
}

type ProdLiveAuctionsState struct {
	State

	LiveAuctionsBase store.LiveAuctionsBase
}
