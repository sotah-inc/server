package fn

import (
	"time"

	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
)

func (sta ComputeAllLiveAuctionsState) PublishToReceiveComputedLiveAuctions(tuples bus.RegionRealmTimestampTuples) error {
	// stripping non-essential data
	bareTuples := bus.RegionRealmTimestampTuples{}
	for _, tuple := range tuples {
		bareTuples = append(bareTuples, tuple.Bare())
	}

	// producing a message for computation
	data, err := bareTuples.EncodeForDelivery()
	if err != nil {
		return err
	}
	msg := bus.NewMessage()
	msg.Data = data

	// publishing to receive-computed-live-auctions
	logging.Info("Publishing to receive-computed-live-auctions")
	if _, err := sta.IO.BusClient.Publish(sta.receiveComputedLiveAuctionsTopic, msg); err != nil {
		return err
	}

	return nil
}

func (sta ComputeAllLiveAuctionsState) Run(data string) error {
	// formatting the response-items as tuples for processing
	tuples, err := bus.NewRegionRealmTimestampTuples(data)
	if err != nil {
		return err
	}

	// producing messages
	logging.WithField("tuples", len(tuples)).Info("Producing messages for bulk requesting")
	messages, err := tuples.ToMessages()
	if err != nil {
		return err
	}

	// enqueueing them and gathering result jobs
	logging.WithField("messages", len(messages)).Info("Enqueueing compute-live-auctions messages")
	startTime := time.Now()
	responseItems, err := sta.IO.BusClient.BulkRequest(sta.computeLiveAuctionsTopic, messages, 120*time.Second)
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
		"compute_all_live_auctions_duration": int(int64(time.Now().Sub(startTime)) / 1000 / 1000 / 1000),
		"included_realms":                    len(validatedResponseItems),
	}); err != nil {
		return err
	}

	// publishing to receive-computed-live-auctions
	if err := sta.PublishToReceiveComputedLiveAuctions(tuples); err != nil {
		return err
	}

	logging.Info("Finished")

	return nil
}
