package command

import (
	"os"
	"os/signal"

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

	// listening for pricelist-histories-compute-intake
	computeIntakeStop := make(chan interface{})
	computeIntakeOnReady := make(chan interface{})
	computeIntakeOnStopped := make(chan interface{})
	if err := pubState.ListenForPricelistHistoriesComputeIntake(computeIntakeStop, computeIntakeOnReady, computeIntakeOnStopped); err != nil {
		return err
	}
	<-computeIntakeOnReady

	// catching SIGINT
	logging.Info("Waiting for SIGINT")
	sigIn := make(chan os.Signal, 1)
	signal.Notify(sigIn, os.Interrupt)
	<-sigIn

	logging.Info("Caught SIGINT, exiting")

	// stopping listeners
	pubState.Listeners.Stop()
	computeIntakeStop <- struct{}{}
	<-computeIntakeOnStopped

	// stopping pruner
	logging.Info("Stopping pruner")
	prunerStop <- struct{}{}

	logging.Info("Waiting for pruner to stop")
	<-onPrunerStop

	logging.Info("Exiting")
	return nil
}
