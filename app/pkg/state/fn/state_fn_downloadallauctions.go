package fn

import (
	"log"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/sotah/gameversions"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/twinj/uuid"
)

type DownloadAllAuctionsStateConfig struct {
	ProjectId string

	MessengerHost string
	MessengerPort int
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
	sta.computeAllLiveAuctionsTopic, err = sta.IO.BusClient.FirmTopic(
		string(subjects.ComputeAllLiveAuctions),
	)
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return DownloadAllAuctionsState{}, err
	}
	sta.computeAllPricelistHistoriesTopic, err = sta.IO.BusClient.FirmTopic(
		string(subjects.ComputeAllPricelistHistories),
	)
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return DownloadAllAuctionsState{}, err
	}

	// connecting to the messenger host
	mess, err := messenger.NewMessenger(config.MessengerHost, config.MessengerPort)
	if err != nil {
		return DownloadAllAuctionsState{}, err
	}
	sta.IO.Messenger = mess

	// initializing a reporter
	sta.IO.Reporter = metric.NewReporter(mess)

	sta.IO.StoreClient, err = store.NewClient(config.ProjectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return DownloadAllAuctionsState{}, err
	}

	sta.bootBase = store.NewBootBase(sta.IO.StoreClient, "us-central1")
	sta.bootBucket, err = sta.bootBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return DownloadAllAuctionsState{}, err
	}

	sta.realmsBase = store.NewRealmsBase(sta.IO.StoreClient, "us-central1", gameversions.Retail)
	sta.realmsBucket, err = sta.realmsBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

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

	bootBase     store.BootBase
	bootBucket   *storage.BucketHandle
	realmsBase   store.RealmsBase
	realmsBucket *storage.BucketHandle

	downloadAuctionsTopic             *pubsub.Topic
	computeAllLiveAuctionsTopic       *pubsub.Topic
	computeAllPricelistHistoriesTopic *pubsub.Topic
}

func (sta DownloadAllAuctionsState) ListenForDownloadAllAuctions(onReady chan interface{}, stop chan interface{}, onStopped chan interface{}) {
	in := make(chan interface{})
	go func() {
		for {
			<-in
			if err := sta.Run(); err != nil {
				logging.WithField("error", err.Error()).Error("Failed to run")
			}
		}
	}()

	// establishing subscriber config
	config := bus.SubscribeConfig{
		Stop: stop,
		Callback: func(busMsg bus.Message) {
			in <- struct{}{}
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
