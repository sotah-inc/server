package command

import (
	"os"
	"os/signal"

	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/state"
)

func ProdMetrics(config state.ProdMetricsStateConfig) error {
	logging.Info("Starting prod-metrics")

	// establishing a state
	metricsState, err := state.NewProdMetricstate(config)
	if err != nil {
		logging.WithField("error", err.Error()).Error("Failed to establish prod-metrics state")

		return err
	}

	// opening all listeners
	if err := metricsState.Listeners.Listen(); err != nil {
		return err
	}

	// opening all bus-listeners
	logging.Info("Opening all bus-listeners")
	metricsState.BusListeners.Listen()

	// catching SIGINT
	logging.Info("Waiting for SIGINT")
	sigIn := make(chan os.Signal, 1)
	signal.Notify(sigIn, os.Interrupt)
	<-sigIn

	logging.Info("Caught SIGINT, exiting")

	// stopping listeners
	metricsState.Listeners.Stop()

	logging.Info("Exiting")
	return nil
}
