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
	"github.com/sotah-inc/server/app/pkg/store/regions"
	"github.com/twinj/uuid"
)

type CleanupAllExpiredManifestsStateConfig struct {
	ProjectId string
}

func NewCleanupAllExpiredManifestsState(
	config CleanupAllExpiredManifestsStateConfig,
) (CleanupAllExpiredManifestsState, error) {
	// establishing an initial state
	sta := CleanupAllExpiredManifestsState{
		State: state.NewState(uuid.NewV4(), true),
	}

	var err error
	sta.IO.BusClient, err = bus.NewClient(config.ProjectId, "fn-cleanup-all-expired-manifests")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return CleanupAllExpiredManifestsState{}, err
	}
	sta.auctionsCleanupTopic, err = sta.IO.BusClient.FirmTopic(string(subjects.CleanupExpiredManifest))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return CleanupAllExpiredManifestsState{}, err
	}

	sta.IO.StoreClient, err = store.NewClient(config.ProjectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return CleanupAllExpiredManifestsState{}, err
	}

	sta.bootBase = store.NewBootBase(sta.IO.StoreClient, regions.USCentral1)
	sta.bootBucket, err = sta.bootBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return CleanupAllExpiredManifestsState{}, err
	}

	sta.realmsBase = store.NewRealmsBase(sta.IO.StoreClient, regions.USCentral1, gameversions.Retail)
	sta.realmsBucket, err = sta.realmsBase.GetFirmBucket()
	if err != nil {
		return CleanupAllExpiredManifestsState{}, err
	}

	sta.auctionManifestStoreBase = store.NewAuctionManifestBaseV2(
		sta.IO.StoreClient,
		regions.USCentral1,
		gameversions.Retail,
	)
	sta.auctionManifestBucket, err = sta.auctionManifestStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm auction-manifest bucket: %s", err.Error())

		return CleanupAllExpiredManifestsState{}, err
	}

	// establishing bus-listeners
	sta.BusListeners = state.NewBusListeners(state.SubjectBusListeners{
		subjects.CleanupAllExpiredManifests: sta.ListenForCleanupAllExpiredManifests,
	})

	return sta, nil
}

type CleanupAllExpiredManifestsState struct {
	state.State

	auctionsCleanupTopic *pubsub.Topic

	auctionManifestStoreBase store.AuctionManifestBaseV2
	auctionManifestBucket    *storage.BucketHandle
	realmsBase               store.RealmsBase
	realmsBucket             *storage.BucketHandle
	bootBase                 store.BootBase
	bootBucket               *storage.BucketHandle
}

func (sta CleanupAllExpiredManifestsState) ListenForCleanupAllExpiredManifests(
	onReady chan interface{},
	stop chan interface{},
	onStopped chan interface{},
) {
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
		if err := sta.IO.BusClient.SubscribeToTopic(string(subjects.CleanupAllExpiredManifests), config); err != nil {
			logging.WithField("error", err.Error()).Fatal("Failed to subscribe to topic")
		}
	}()
}
