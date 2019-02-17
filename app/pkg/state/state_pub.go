package state

import (
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/resolver"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/twinj/uuid"
)

type PubStateConfig struct {
	GCloudProjectID string

	MessengerHost string
	MessengerPort int

	BlizzardClientId     string
	BlizzardClientSecret string
}

func NewPubState(config PubStateConfig) (PubState, error) {
	// establishing an initial state
	pubState := PubState{
		State: NewState(uuid.NewV4(), true),
	}

	// connecting to the messenger host
	logging.Info("Connecting messenger")
	mess, err := messenger.NewMessenger(config.MessengerHost, config.MessengerPort)
	if err != nil {
		return PubState{}, err
	}
	pubState.IO.Messenger = mess

	// initializing a reporter
	pubState.IO.Reporter = metric.NewReporter(mess)

	// gathering regions
	logging.Info("Gathering regions")
	regions, err := pubState.NewRegions()
	if err != nil {
		return PubState{}, err
	}
	pubState.Regions = regions

	// gathering statuses
	logging.Info("Gathering statuses")
	for _, reg := range pubState.Regions {
		status, err := pubState.NewStatus(reg)
		if err != nil {
			return PubState{}, err
		}

		pubState.Statuses[reg.Name] = status
	}

	// connecting a new blizzard client
	blizzardClient, err := blizzard.NewClient(config.BlizzardClientId, config.BlizzardClientSecret)
	if err != nil {
		return PubState{}, err
	}
	pubState.IO.Resolver = resolver.NewResolver(blizzardClient, pubState.IO.Reporter)

	// establishing a bus
	bu, err := bus.NewClient(config.GCloudProjectID, "pub")
	pubState.IO.BusClient = bu

	// establishing a store
	stor, err := store.NewClient(config.GCloudProjectID)
	if err != nil {
		return PubState{}, err
	}
	pubState.IO.StoreClient = stor

	// establishing listeners
	pubState.Listeners = NewListeners(SubjectListeners{
		subjects.AuctionCount: pubState.ListenForAuctionCount,
	})

	return pubState, nil
}

type PubState struct {
	State
}

func (pubState PubState) ListenForAuctionCount(stop ListenStopChan) error {
	startTime := time.Now()
	totalAuctions := 0
	timeout := 5 * time.Minute

	in := make(chan bus.Message)
	logging.Info("Spawning channel for receiving responses")
	go func() {
		for {
			select {
			case <-time.After(timeout):
				duration := time.Now().Sub(startTime)

				logging.WithFields(logrus.Fields{
					"total_auctions": totalAuctions,
					"duration":       int(duration) / 1000 / 1000,
				}).Info("Counted auctions")

				return
			case msg := <-in:
				count, err := strconv.Atoi(msg.Data)
				if err != nil {
					logging.WithFields(logrus.Fields{
						"error": err.Error(),
						"data":  msg.Data,
					}).Error("Failed to parse data")

					continue
				}

				duration := time.Now().Sub(startTime)
				totalAuctions += count

				logging.WithFields(logrus.Fields{
					"total_auctions": totalAuctions,
					"duration":       int(duration) / 1000 / 1000,
				}).Info("Counted auctions")
			}
		}
	}()

	logging.Info("Calling subscribeToTopic")
	err := pubState.IO.BusClient.SubscribeToTopic(string(subjects.AuctionCount), stop, func(busMsg bus.Message) {
		logging.WithField("subject", subjects.AuctionCount).Info("Received message")

		in <- busMsg

		return
	})
	if err != nil {
		return err
	}

	return nil
}
