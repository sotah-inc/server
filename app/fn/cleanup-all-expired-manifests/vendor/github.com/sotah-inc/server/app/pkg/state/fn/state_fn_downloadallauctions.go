package fn

import (
	"log"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/twinj/uuid"
)

type DownloadAllAuctionsStateConfig struct {
	ProjectId string
}

func NewDownloadAllAuctionsState(config DownloadAllAuctionsStateConfig) (DownloadAllAuctionsState, error) {
	// establishing an initial state
	sta := DownloadAllAuctionsState{
		State: state.NewState(uuid.NewV4(), true),
	}

	var err error
	sta.IO.BusClient, err = bus.NewClient(config.ProjectId, "fn-download-all-auctions")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return DownloadAllAuctionsState{}, err
	}
	sta.downloadAuctionsTopic, err = sta.IO.BusClient.FirmTopic(string(subjects.DownloadAuctions))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return DownloadAllAuctionsState{}, err
	}
	sta.syncAllItemsTopic, err = sta.IO.BusClient.FirmTopic(string(subjects.SyncAllItems))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return DownloadAllAuctionsState{}, err
	}
	sta.receivedComputedLiveAuctionsTopic, err = sta.IO.BusClient.FirmTopic(string(subjects.ReceiveComputedLiveAuctions))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return DownloadAllAuctionsState{}, err
	}
	sta.receivedComputedPricelistHistoriesTopic, err = sta.IO.BusClient.FirmTopic(string(subjects.ReceiveComputedPricelistHistories))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return DownloadAllAuctionsState{}, err
	}

	sta.IO.StoreClient, err = store.NewClient(config.ProjectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return DownloadAllAuctionsState{}, err
	}

	bootBase := store.NewBootBase(sta.IO.StoreClient, "us-central1")
	var bootBucket *storage.BucketHandle
	bootBucket, err = bootBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return DownloadAllAuctionsState{}, err
	}
	sta.regionRealms, err = bootBase.GetRegionRealms(bootBucket)
	if err != nil {
		log.Fatalf("Failed to get region-realms: %s", err.Error())

		return DownloadAllAuctionsState{}, err
	}

	// establishing bus-listeners
	sta.BusListeners = state.NewBusListeners(state.SubjectBusListeners{
		subjects.DownloadAllAuctions: sta.ListenForDownloadAllAuctions,
	})

	return sta, nil
}

type DownloadAllAuctionsState struct {
	state.State

	regionRealms map[blizzard.RegionName]sotah.Realms

	downloadAuctionsTopic                   *pubsub.Topic
	syncAllItemsTopic                       *pubsub.Topic
	receivedComputedLiveAuctionsTopic       *pubsub.Topic
	receivedComputedPricelistHistoriesTopic *pubsub.Topic
}

func (sta DownloadAllAuctionsState) ListenForDownloadAllAuctions(onReady chan interface{}, stop chan interface{}, onStopped chan interface{}) {
	// establishing subscriber config
	config := bus.SubscribeConfig{
		Stop: stop,
		Callback: func(busMsg bus.Message) {
			if err := sta.Run(); err != nil {
				logging.WithField("error", err.Error()).Error("Failed to run")
			}
		},
		OnReady:   onReady,
		OnStopped: onStopped,
	}

	// starting up worker for the subscription
	go func() {
		if err := sta.IO.BusClient.SubscribeToTopic(string(subjects.DownloadAllAuctions), config); err != nil {
			logging.WithField("error", err.Error()).Fatal("Failed to subscribe to topic")
		}
	}()
}
