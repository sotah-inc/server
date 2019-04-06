package fn

import (
	"encoding/json"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/sotah"
)

func (sta DownloadAllAuctionsState) PublishToSyncAllItems(tuples bus.RegionRealmTimestampTuples) error {
	itemIdsMap := sotah.ItemIdsMap{}
	for _, tuple := range tuples {
		for _, id := range tuple.ItemIds {
			itemIdsMap[blizzard.ItemID(id)] = struct{}{}
		}
	}
	itemIds := blizzard.ItemIds{}
	for id := range itemIdsMap {
		itemIds = append(itemIds, id)
	}

	// producing a item-ids message for syncing
	data, err := itemIds.EncodeForDelivery()
	if err != nil {
		return err
	}
	msg := bus.NewMessage()
	msg.Data = data

	// publishing to sync-all-items
	logging.Info("Publishing to sync-all-items")
	if _, err := sta.IO.BusClient.Publish(sta.syncAllItemsTopic, msg); err != nil {
		return err
	}

	return nil
}

func (sta DownloadAllAuctionsState) PublishToReceiveComputedLiveAuctions(tuples bus.RegionRealmTimestampTuples) error {
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
	if _, err := sta.IO.BusClient.Publish(sta.receivedComputedLiveAuctionsTopic, msg); err != nil {
		return err
	}

	return nil
}

func (sta DownloadAllAuctionsState) PublishToReceivePricelistHistories(tuples bus.RegionRealmTimestampTuples) error {
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
	if _, err := sta.IO.BusClient.Publish(sta.receivedComputedPricelistHistoriesTopic, msg); err != nil {
		return err
	}

	return nil
}

func (sta DownloadAllAuctionsState) Run() error {
	// producing messages
	logging.Info("Producing messages for bulk requesting")
	messages, err := bus.NewCollectAuctionMessages(sta.regionRealms)
	if err != nil {
		return err
	}

	// enqueueing them and gathering result jobs
	startTime := time.Now()
	responseItems, err := sta.IO.BusClient.BulkRequest(sta.downloadAuctionsTopic, messages, 400*time.Second)
	if err != nil {
		return err
	}

	validatedResponseItems := bus.BulkRequestMessages{}
	for k, msg := range responseItems {
		if msg.Code != codes.Ok {
			if msg.Code == codes.BlizzardError {
				var respError blizzard.ResponseError
				if err := json.Unmarshal([]byte(msg.Data), &respError); err != nil {
					return err
				}

				logging.WithFields(logrus.Fields{"resp-error": respError}).Error("Received erroneous response")
			}

			continue
		}

		// ok msg code but no msg data means no new auctions
		if len(msg.Data) == 0 {
			continue
		}

		validatedResponseItems[k] = msg
	}

	// reporting metrics
	if err := sta.IO.BusClient.PublishMetrics(metric.Metrics{
		"download_all_auctions_duration": int(int64(time.Now().Sub(startTime)) / 1000 / 1000 / 1000),
		"included_realms":                len(validatedResponseItems),
	}); err != nil {
		return err
	}

	// formatting the response-items as tuples for processing
	tuples, err := bus.NewRegionRealmTimestampTuplesFromMessages(validatedResponseItems)
	if err != nil {
		return err
	}

	// publishing to sync-all-items
	if err := sta.PublishToSyncAllItems(tuples); err != nil {
		return err
	}

	// publishing to receive-computed-live-auctions
	if err := sta.PublishToReceiveComputedLiveAuctions(tuples); err != nil {
		return err
	}

	// publishing to receive-live-auctions
	if err := sta.PublishToReceivePricelistHistories(tuples); err != nil {
		return err
	}

	logging.Info("Finished")

	return nil
}
