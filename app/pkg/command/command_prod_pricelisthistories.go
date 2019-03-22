package command

import (
	"os"
	"os/signal"

	"github.com/sotah-inc/server/app/pkg/logging"
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
	if err := pricelistHistoriesState.Sync(); err != nil {
		logging.WithField("error", err.Error()).Error("Failed to sync pricelist-histories db with pricelist-histories base")

		return err
	}

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

	logging.Info("Exiting")
	return nil
}
