package command

import (
	"encoding/json"
	"os"
	"os/signal"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/bus"
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
	go func() {
		logging.Info("Calling Listeners.Listen()")
		if err := pubState.Listeners.Listen(); err != nil {
			logging.WithField("error", err.Error()).Fatal("Failed to open listeners")

			return
		}
	}()

	// queueing up all realms
	logging.Info("Queueing up realms")
	for _, status := range pubState.Statuses {
		for _, realm := range status.Realms {
			jsonEncoded, err := json.Marshal(realm)
			if err != nil {
				return err
			}

			msg := bus.NewMessage()
			msg.Data = string(jsonEncoded)

			logging.WithFields(logrus.Fields{
				"region": realm.Region.Name,
				"realm":  realm.Slug,
			}).Info("Queueing up realm")
			if _, err := pubState.IO.BusClient.Publish(pubState.IO.BusClient.Topic(string(subjects.AuctionCount)), msg); err != nil {
				return err
			}

			break
		}

		break
	}

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
