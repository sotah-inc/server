package state

import (
	"cloud.google.com/go/pubsub"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/twinj/uuid"
)

type TestStateConfig struct {
	GCloudProjectID string

	MessengerHost string
	MessengerPort int
}

func NewTestState(config TestStateConfig) (TestState, error) {
	// establishing an initial state
	tState := TestState{
		State: NewState(uuid.NewV4(), true),
	}

	// connecting to the messenger host
	logging.Info("Connecting messenger")
	mess, err := messenger.NewMessenger(config.MessengerHost, config.MessengerPort)
	if err != nil {
		return TestState{}, err
	}
	tState.IO.Messenger = mess

	// establishing a bus
	bu, err := bus.NewBus(config.GCloudProjectID)
	tState.IO.Bus = bu

	// initializing a reporter
	tState.IO.Reporter = metric.NewReporter(mess)

	// gathering regions
	logging.Info("Gathering regions")
	regions, err := tState.NewRegions()
	if err != nil {
		return TestState{}, err
	}
	tState.Regions = regions

	// establishing listeners
	tState.Listeners = NewListeners(SubjectListeners{
		subjects.Boot: tState.ListenForBoot,
	})

	return tState, nil
}

type TestState struct {
	State
}

func (tState TestState) ListenForBoot(stop ListenStopChan) error {
	err := tState.IO.Bus.Subscribe(string(subjects.Boot), stop, func(msg pubsub.Message) {
		return
	})
	if err != nil {
		return err
	}

	return nil
}
