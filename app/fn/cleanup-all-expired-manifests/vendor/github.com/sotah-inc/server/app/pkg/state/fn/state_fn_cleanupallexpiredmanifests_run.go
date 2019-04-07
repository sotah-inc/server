package fn

import (
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
)

func (sta CleanupAllExpiredManifestsState) Run() error {
	logging.Info("Starting CleanupAllExpiredManifests.Run()")

	regionExpiredTimestamps, err := sta.auctionManifestStoreBase.GetAllExpiredTimestamps(
		sta.regionRealms,
		sta.auctionManifestBucket,
	)
	if err != nil {
		return err
	}

	jobs := []bus.CleanupAuctionManifestJob{}
	for regionName, realmExpiredTimestamps := range regionExpiredTimestamps {
		for realmSlug, expiredTimestamps := range realmExpiredTimestamps {
			for _, timestamp := range expiredTimestamps {
				job := bus.CleanupAuctionManifestJob{
					RegionName:      string(regionName),
					RealmSlug:       string(realmSlug),
					TargetTimestamp: int(timestamp),
				}
				jobs = append(jobs, job)
			}
		}
	}

	for outJob := range sta.IO.BusClient.LoadAuctionsCleanupJobs(jobs, sta.auctionsCleanupTopic) {
		if outJob.Err != nil {
			return outJob.Err
		}
	}

	logging.Info("Finished CleanupAllExpiredManifests.Run()")

	return nil
}
