package fn

import (
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
)

func (sta CleanupAllExpiredManifestsState) Run() error {
	logging.Info("Starting CleanupAllExpiredManifests.Run()")

	logging.Info("Gathering expired timestamps")
	regionExpiredTimestamps, err := sta.auctionManifestStoreBase.GetAllExpiredTimestamps(
		sta.regionRealms,
		sta.auctionManifestBucket,
	)
	if err != nil {
		return err
	}

	jobs := bus.NewCleanupAuctionManifestJobs(regionExpiredTimestamps)
	for outJob := range sta.IO.BusClient.LoadAuctionsCleanupJobs(jobs, sta.auctionsCleanupTopic) {
		if outJob.Err != nil {
			return outJob.Err
		}
	}

	logging.Info("Finished CleanupAllExpiredManifests.Run()")

	return nil
}
