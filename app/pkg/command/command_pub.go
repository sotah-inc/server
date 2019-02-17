package command

import (
	"os"
	"os/signal"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/state"
)

func Pub(config state.PubStateConfig) error {
	logging.Info("Starting pub")

	// establishing a state
	pubState, err := state.NewPubState(config)
	if err != nil {
		return err
	}

	// checking which realms don't have auctions
	bkt := pubState.IO.Store.GetTestAuctionsBucket()
	for _, status := range pubState.Statuses {
		for _, realm := range status.Realms {
			exists, err := pubState.IO.Store.TestAuctionsObjectExists(bkt, realm)
			if err != nil {
				return err
			}

			if exists {
				continue
			}

			logging.WithFields(logrus.Fields{
				"region": realm.Region.Name,
				"realm":  realm.Slug,
			}).Warning("Realm does not have auctions obj")
		}
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
