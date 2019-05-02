package fn

import (
	"time"

	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
)

func (sta ComputeAllPricelistHistoriesState) PublishToReceivePricelistHistories(tuples bus.RegionRealmTimestampTuples) error {
	// producing pricelist-histories-compute-intake-requests
	requests := database.PricelistHistoriesComputeIntakeRequests{}
	for _, tuple := range tuples {
		requests = append(requests, database.PricelistHistoriesComputeIntakeRequest{
			RegionName:                tuple.RegionName,
			RealmSlug:                 tuple.RealmSlug,
			NormalizedTargetTimestamp: tuple.NormalizedTargetTimestamp,
		})
	}

	// producing a message for computation
	data, err := requests.EncodeForDelivery()
	if err != nil {
		return err
	}
	msg := bus.NewMessage()
	msg.Data = data

	// publishing to receive-computed-pricelist-histories
	logging.Info("Publishing to receive-computed-pricelist-histories")
	if _, err := sta.IO.BusClient.Publish(sta.receiveComputedPricelistHistoriesTopic, msg); err != nil {
		return err
	}

	return nil
}

func (sta ComputeAllPricelistHistoriesState) Run(data string) error {
	// formatting the response-items as tuples for processing
	tuples, err := bus.NewRegionRealmTimestampTuples(data)
	if err != nil {
		return err
	}

	// producing messages
	logging.Info("Producing messages for bulk requesting")
	messages, err := tuples.ToMessages()
	if err != nil {
		return err
	}

	// enqueueing them and gathering result jobs
	startTime := time.Now()
	responseItems, err := sta.IO.BusClient.BulkRequest(sta.computePricelistHistoriesTopic, messages, 400*time.Second)
	if err != nil {
		return err
	}

	validatedResponseItems := bus.BulkRequestMessages{}
	for k, msg := range responseItems {
		if msg.Code != codes.Ok {
			continue
		}

		validatedResponseItems[k] = msg
	}

	// reporting metrics
	if err := sta.IO.BusClient.PublishMetrics(metric.Metrics{
		"compute_all_pricelist_histories_duration": int(int64(time.Now().Sub(startTime)) / 1000 / 1000 / 1000),
		"included_realms":                          len(validatedResponseItems),
	}); err != nil {
		return err
	}

	// publishing to receive-computed-pricelist-histories
	if err := sta.PublishToReceiveComputedPricelistHistories(tuples); err != nil {
		return err
	}

	logging.Info("Finished")

	return nil
}
