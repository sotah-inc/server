package fn

import (
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/sotah"
)

func (sta CleanupAllExpiredManifestsState) Run() error {
	logging.Info("Starting CleanupAllExpiredManifests.Run()")

	regions, err := sta.bootBase.GetRegions(sta.bootBucket)
	if err != nil {
		return err
	}

	logging.WithField("regions", len(regions)).Info("Found regions")

	regionRealms := sotah.RegionRealms{}
	for _, region := range regions {
		realms, err := sta.realmsBase.GetAllRealms(region.Name, sta.realmsBucket)
		if err != nil {
			return err
		}

		logging.WithFields(logrus.Fields{
			"region": region.Name,
			"realms": len(realms),
		}).Info("Found realms")

		regionRealms[region.Name] = realms
	}

	logging.WithField("realms", regionRealms.TotalRealms()).Info("Gathering expired timestamps")
	regionExpiredTimestamps, err := sta.auctionManifestStoreBase.GetAllExpiredTimestamps(
		regionRealms,
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

	if len(messages) == 0 {
		logging.Info("No expired-manifest job messages found, exiting early")

		return nil
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
