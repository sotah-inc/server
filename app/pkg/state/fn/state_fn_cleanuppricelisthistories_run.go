package fn

import (
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/sotah/gameversions"
	"github.com/sotah-inc/server/app/pkg/store"
)

func (sta CleanupPricelistHistoriesState) Run() error {
	logging.Info("Starting CleanupPricelistHistories.Run()")

	regions, err := sta.bootStoreBase.GetRegions(sta.bootBucket)
	if err != nil {
		return err
	}

	realmsBase := store.NewRealmsBase(sta.IO.StoreClient, "us-central1", gameversions.Retail)
	realmsBucket, err := realmsBase.GetFirmBucket()
	if err != nil {
		return err
	}

	regionRealms := sotah.RegionRealms{}
	for _, region := range regions {
		realms, err := realmsBase.GetRealms(region.Name, realmsBucket)
		if err != nil {
			return err
		}

		regionRealms[region.Name] = realms
	}

	payloads := sotah.NewCleanupPricelistPayloads(regionRealms)
	messages, err := bus.NewCleanupPricelistPayloadsMessages(payloads)
	if err != nil {
		return err
	}

	responses, err := sta.IO.BusClient.BulkRequest(sta.pricelistsCleanupTopic, messages, 120*time.Second)
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

		jobResponse, err := sotah.NewCleanupPricelistPayloadResponse(res.Data)
		if err != nil {
			return err
		}

		totalRemoved += jobResponse.TotalDeleted
	}

	if err := sta.IO.BusClient.PublishMetrics(metric.Metrics{"total_pricelist_histories_removed": totalRemoved}); err != nil {
		return err
	}

	logging.WithField("total-removed", totalRemoved).Info("Finished CleanupPricelistHistories.Run()")

	return nil
}
