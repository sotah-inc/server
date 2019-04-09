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

func (sta CleanupPricelistHistoriesState) Run() error {
	logging.Info("Starting CleanupPricelistHistories.Run()")

	regionRealms, err := sta.bootStoreBase.GetRegionRealms(sta.bootBucket)
	if err != nil {
		return err
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
