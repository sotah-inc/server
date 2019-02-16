package command

import (
	"os"
	"os/signal"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/store"
)

func Pub(config state.PubStateConfig) error {
	logging.Info("Starting pub")

	// establishing a state
	pubState, err := state.NewPubState(config)
	if err != nil {
		return err
	}

	// starting channels for persisting auctions
	logging.Info("Starting store LoadTestAuctions worker")
	loadTestAuctionsIn := make(chan store.LoadAuctionsInJob)
	loadTestAuctionsOut := pubState.IO.Store.LoadTestAuctions(loadTestAuctionsIn)

	// gathering auctions
	logging.Info("Spinning up worker goroutine for fetching auctions")
	go func() {
		for _, status := range pubState.Statuses {
			getAuctionsForRealmsOut := pubState.IO.Resolver.GetAuctionsForRealms(status.Realms)

			for outJob := range getAuctionsForRealmsOut {
				if outJob.Err != nil {
					logging.WithFields(outJob.ToLogrusFields()).Error("Failed to get realms")

					continue
				}

				loadTestAuctionsIn <- store.LoadAuctionsInJob{
					Realm:      outJob.Realm,
					Auctions:   outJob.Auctions,
					TargetTime: outJob.LastModified,
				}
			}
		}
	}()

	// waiting to the jobs to drain out
	logging.Info("Waiting for store load test auctions to drain out")
	for outJob := range loadTestAuctionsOut {
		if outJob.Err != nil {
			return err
		}

		logging.WithFields(logrus.Fields{
			"region":      outJob.Realm.Region.Name,
			"realm":       outJob.Realm.Slug,
			"target-time": outJob.TargetTime.Unix(),
		}).Info("Wrote auctions to realm bucket")
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
