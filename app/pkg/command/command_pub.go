package command

import (
	"errors"
	"github.com/sotah-inc/server/app/pkg/store"
	"google.golang.org/api/iterator"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state"
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

	// resolving the pricelist-histories store-base
	pricelistHistoriesBase := store.NewPricelistHistoriesBase(pubState.IO.StoreClient)

	// gathering a realm
	realm := func() sotah.Realm {
		for regionName, status := range pubState.Statuses {
			if regionName != "us" {
				continue
			}

			for _, rea := range status.Realms {
				if rea.Slug != "earthen-ring" {
					continue
				}

				return rea
			}
		}

		return sotah.Realm{}
	}()
	if realm.Slug == "" {
		return errors.New("could not resolve realm")
	}

	// going over all auctions in a realm
	bkt := pubState.IO.StoreClient.GetRealmAuctionsBucket(realm)
	it := bkt.Objects(pubState.IO.StoreClient.Context, nil)
	i := 0
	for {
		objAttrs, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}

			return err
		}

		obj := bkt.Object(objAttrs.Name)
		targetTime, err := func() (time.Time, error) {
			s := strings.Split(objAttrs.Name, ".")
			targetTimestamp, err := strconv.Atoi(s[0])
			if err != nil {
				return time.Time{}, err
			}

			targetTime := time.Unix(int64(targetTimestamp), 0)

			return targetTime, nil
		}()
		if err != nil {
			return err
		}

		aucs, err := pubState.IO.StoreClient.NewAuctions(obj)
		if err != nil {
			return err
		}

		if err := pricelistHistoriesBase.Handle(aucs, targetTime, realm); err != nil {
			return err
		}

		i++

		if i > 10 {
			break
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

	// stopping pruner
	logging.Info("Stopping pruner")
	prunerStop <- struct{}{}

	logging.Info("Waiting for pruner to stop")
	<-onPrunerStop

	logging.Info("Exiting")
	return nil
}
