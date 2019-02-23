package command

import (
	"os"
	"os/signal"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/store"
	"google.golang.org/api/iterator"
)

func Pub(config state.PubStateConfig) error {
	logging.Info("Starting pub")

	// establishing a state
	pubState, err := state.NewPubState(config)
	if err != nil {
		return err
	}

	// starting up a pruner
	logging.Info("Starting up the pricelist-histories file pruner")
	prunerStop := make(sotah.WorkerStopChan)
	onPrunerStop := pubState.IO.Databases.PricelistHistoryDatabasesV2.StartPruner(prunerStop)

	// opening all listeners
	logging.Info("Opening all listeners")
	if err := pubState.Listeners.Listen(); err != nil {
		return err
	}

	// opening all bus-listeners
	logging.Info("Opening all bus-listeners")
	pubState.BusListeners.Listen()

	// going over all pricelist-history-base objects and clearing them out
	phBase := store.NewPricelistHistoriesBase(pubState.IO.StoreClient)
	for _, status := range pubState.Statuses {
		for _, realm := range status.Realms {
			logging.WithFields(logrus.Fields{
				"region": realm.Region.Name,
				"realm":  realm.Slug,
			}).Info("Clearing bucket")

			bkt := phBase.GetBucket(realm)
			it := bkt.Objects(pubState.IO.StoreClient.Context, nil)
			for {
				objAttrs, err := it.Next()
				if err != nil {
					if err == iterator.Done {
						break
					}

					return err
				}

				obj := bkt.Object(objAttrs.Name)
				if err := obj.Delete(pubState.IO.StoreClient.Context); err != nil {
					return err
				}
			}
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

	// stopping bus-listeners
	pubState.BusListeners.Stop()

	// stopping pruner
	logging.Info("Stopping pruner")
	prunerStop <- struct{}{}

	logging.Info("Waiting for pruner to stop")
	<-onPrunerStop

	logging.Info("Exiting")
	return nil
}
