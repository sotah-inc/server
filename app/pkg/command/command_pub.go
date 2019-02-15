package command

import (
	"os"
	"os/signal"
	"time"

	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func Pub(config state.PubStateConfig) error {
	logging.Info("Starting pub")

	// establishing a state
	pubState, err := state.NewPubState(config)
	if err != nil {
		return err
	}

	// opening all listeners
	if err := pubState.Listeners.Listen(); err != nil {
		return err
	}

	// sending a message
	msg, err := pubState.IO.BusClient.RequestFromTopic(string(subjects.Boot), "world", 5*time.Second)
	if err != nil {
		return err
	}

	logging.WithField("data", msg.Data).Info("Received data")

	// catching SIGINT
	logging.Info("Waiting for SIGINT")
	sigIn := make(chan os.Signal, 1)
	signal.Notify(sigIn, os.Interrupt)
	<-sigIn

	logging.Info("Caught SIGINT, exiting")

	// stopping listeners
	pubState.Listeners.Stop()

	logging.Info("Exiting")
	return nil
}
