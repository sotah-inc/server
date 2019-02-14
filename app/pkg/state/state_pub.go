package state

import (
	"fmt"

	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/twinj/uuid"
)

type PubStateConfig struct {
	GCloudProjectID string

	MessengerHost string
	MessengerPort int
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

	// establishing a bus
	bu, err := bus.NewBus(config.GCloudProjectID, "pub")
	pubState.IO.Bus = bu

	// initializing a reporter
	pubState.IO.Reporter = metric.NewReporter(mess)

	// gathering regions
	logging.Info("Gathering regions")
	regions, err := pubState.NewRegions()
	if err != nil {
		return PubState{}, err
	}
	pubState.Regions = regions

	// establishing listeners
	pubState.Listeners = NewListeners(SubjectListeners{
		subjects.Boot: pubState.ListenForBoot,
	})

	return pubState, nil
}

type PubState struct {
	State
}

func (pubState PubState) ListenForBoot(stop ListenStopChan) error {
	err := pubState.IO.Bus.SubscribeToTopic(string(subjects.Boot), stop, func(busMsg bus.Message) {
		logging.WithField("subject", subjects.Boot).Info("Received message")

		msg := bus.NewMessage()
		msg.Data = fmt.Sprintf("Hello, %s!", busMsg.Data)
		if _, err := pubState.IO.Bus.ReplyTo(busMsg, msg); err != nil {
			logging.WithField("error", err.Error()).Error("Failed to reply to response message")

			return
		}

		return
	})
	if err != nil {
		return err
	}

	return nil
}
