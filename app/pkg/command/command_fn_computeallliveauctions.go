package command

import (
	"os"
	"os/signal"

	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/state/fn"
)

func FnComputeAllLiveAuctions(config fn.ComputeAllLiveAuctionsStateConfig) error {
	logging.Info("Starting fn-compute-all-live-auctions")

	// establishing a state
	apiState, err := fn.NewComputeAllLiveAuctionsState(config)
	if err != nil {
		logging.WithField("error", err.Error()).Error("Failed to establish fn-compute-all-live-auctions")

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
