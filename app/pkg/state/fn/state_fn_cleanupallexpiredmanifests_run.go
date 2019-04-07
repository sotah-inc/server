package fn

import (
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
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

	logging.Info("Converting to jobs and jobs messages")
	jobs := bus.NewCleanupAuctionManifestJobs(regionExpiredTimestamps)
	messages, err := bus.NewCleanupAuctionManifestJobsMessages(jobs)
	if err != nil {
		return err
	}

	logging.Info("Bulk publishing")
	responses, err := sta.IO.BusClient.BulkRequest(sta.auctionsCleanupTopic, messages, 120*time.Second)
	if err != nil {
		return err
	}

	totalRemoved := 0
	for _, res := range responses {
		if res.Code != codes.Ok {
			logging.WithFields(logrus.Fields{
				"error":       res.Err,
				"reply-to-id": res.ReplyToId,
			}).Error("Job failure")

			continue
		}

		jobResponse, err := bus.NewCleanupAuctionManifestJobResponse(res.Data)
		if err != nil {
			return err
		}

		totalRemoved += jobResponse.TotalDeleted
	}

	if err := sta.IO.BusClient.PublishMetrics(metric.Metrics{"total_expired_manifests_removed": totalRemoved}); err != nil {
		return err
	}

	logging.WithField("total-removed", totalRemoved).Info("Finished CleanupAllExpiredManifests.Run()")

	return nil
}
