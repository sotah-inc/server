package command

import (
	"os"
	"os/signal"

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

	totalAuctions := 0
	for _, status := range pubState.Statuses {
		for job := range pubState.IO.Store.GetTestAuctionsFromRealms(status.Realms) {
			if job.Err != nil {
				return err
			}

			totalAuctions += len(job.Auctions.Auctions)
		}
	}
	logging.WithField("auctions", totalAuctions).Info("Finished counting auctions")

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
