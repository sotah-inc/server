package command

import (
	"os"
	"os/signal"
	"time"

	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state"
)

func ProdPricelistHistories(config state.ProdPricelistHistoriesStateConfig) error {
	logging.Info("Starting prod-metrics")

	// establishing a state
	pricelistHistoriesState, err := state.NewProdPricelistHistoriesState(config)
	if err != nil {
		logging.WithField("error", err.Error()).Error("Failed to establish prod-pricelisthistories state")

		return err
	}

	// syncing local pricelist-histories with base pricelist-histories
	startTime := time.Now()
	//if err := pricelistHistoriesState.Sync(); err != nil {
	//	logging.WithField("error", err.Error()).Error("Failed to sync pricelist-histories db with pricelist-histories base")
	//
	//	return err
	//}

	// reporting sync duration
	m := metric.Metrics{"pricelist_histories_sync": int(int64(time.Now().Sub(startTime)) / 1000 / 1000 / 1000)}
	if err := pricelistHistoriesState.IO.BusClient.PublishMetrics(m); err != nil {
		logging.WithField("error", err.Error()).Error("Failed to publish metric")

		return err
	}

	// starting up a pruner
	logging.Info("Starting up the pricelist-histories file pruner")
	prunerStop := make(sotah.WorkerStopChan)
	onPrunerStop := pricelistHistoriesState.IO.Databases.PricelistHistoryDatabases.StartPruner(prunerStop)

	// opening all listeners
	if err := pricelistHistoriesState.Listeners.Listen(); err != nil {
		return err
	}

	// opening all bus-listeners
	logging.Info("Opening all bus-listeners")
	pricelistHistoriesState.BusListeners.Listen()

	// catching SIGINT
	logging.Info("Waiting for SIGINT")
	sigIn := make(chan os.Signal, 1)
	signal.Notify(sigIn, os.Interrupt)
	<-sigIn

	logging.Info("Caught SIGINT, exiting")

	// stopping listeners
	pricelistHistoriesState.Listeners.Stop()

	// stopping pruner
	logging.Info("Stopping pruner")
	prunerStop <- struct{}{}

	logging.Info("Waiting for pruner to stop")
	<-onPrunerStop

	logging.Info("Exiting")
	return nil
}
