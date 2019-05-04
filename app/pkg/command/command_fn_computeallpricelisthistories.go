package command

import (
	"os"
	"os/signal"

	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/state/fn"
)

func FnComputeAllPricelistHistories(config fn.ComputeAllPricelistHistoriesStateConfig) error {
	logging.Info("Starting fn-compute-all-pricelist-histories")

	// establishing a state
	apiState, err := fn.NewComputeAllPricelistHistoriesState(config)
	if err != nil {
		logging.WithField("error", err.Error()).Error("Failed to establish fn-compute-all-pricelist-histories")

		return err
	}

	// opening all listeners
	if err := apiState.Listeners.Listen(); err != nil {
		return err
	}

	// opening all bus-listeners
	logging.Info("Opening all bus-listeners")
	apiState.BusListeners.Listen()

	// catching SIGINT
	logging.Info("Waiting for SIGINT")
	sigIn := make(chan os.Signal, 1)
	signal.Notify(sigIn, os.Interrupt)
	<-sigIn

	logging.Info("Caught SIGINT, exiting")

	// stopping listeners
	apiState.Listeners.Stop()
	apiState.BusListeners.Stop()

	logging.Info("Exiting")
	return nil
}
