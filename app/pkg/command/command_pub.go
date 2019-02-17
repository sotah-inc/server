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

	// starting a listener
	stop := make(chan interface{})
	onReady := make(chan interface{})
	onStopped := make(chan interface{})
	go func() {
		if err := pubState.ListenForAuctionCount(stop, onReady, onStopped); err != nil {
			logging.WithField("error", err.Error()).Fatal("Failed to start listener")
		}
	}()

	// waiting for the listener to start
	<-onReady

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
	stop <- struct{}{}
	<-onStopped

	logging.Info("Exiting")
	return nil
}
