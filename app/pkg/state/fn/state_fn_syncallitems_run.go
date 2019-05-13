package fn

import (
	"errors"
	"time"

	"github.com/sotah-inc/server/app/pkg/database"

	"github.com/sotah-inc/server/app/pkg/sotah"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
)

func (sta SyncAllItemsState) HandleItemIds(ids blizzard.ItemIds) error {
	if len(ids) == 0 {
		logging.Info("No item-ids in sync-payload, skipping")

		return nil
	}

	// batching items together
	logging.WithField("ids", len(ids)).Info("Batching ids together")
	itemIdsBatches := sotah.NewItemIdsBatches(ids, 1000)

	// producing messages
	logging.WithField("batches", len(itemIdsBatches)).Info("Producing messages for enqueueing")
	messages, err := bus.NewItemBatchesMessages(itemIdsBatches)
	if err != nil {
		return err
	}

	// enqueueing them
	logging.WithField("messages", len(messages)).Info("Bulk-requesting with messages")
	responses, err := sta.IO.BusClient.BulkRequest(sta.syncItemsTopic, messages, 60*time.Second)
	if err != nil {
		return err
	}

	// going over the responses
	logging.WithField("responses", len(responses)).Info("Going over responses")
	for _, msg := range responses {
		if msg.Code != codes.Ok {
			logging.WithField("error", msg.Err).Error("Request from sync-items failed")

			continue
		}

		logging.WithField("batch", msg.ReplyToId).Info("Finished batch")
	}

	return nil
}

func (sta SyncAllItemsState) HandleItemIcons(iconsMap map[string]blizzard.ItemIds) error {
	if len(iconsMap) == 0 {
		logging.Info("No icons in sync-payload, skipping")

		return nil
	}

	// batching icons together
	logging.WithField("icons", len(iconsMap)).Info("Batching icons together")
	iconBatches := sotah.NewIconItemsPayloadsBatches(iconsMap, 100)

	// producing messages
	logging.WithField("batches", len(iconBatches)).Info("Producing messages for enqueueing")
	messages, err := bus.NewItemIconBatchesMessages(iconBatches)
	if err != nil {
		return err
	}

	// enqueueing them
	logging.WithField("messages", len(messages)).Info("Bulk-requesting with messages")
	responses, err := sta.IO.BusClient.BulkRequest(sta.syncItemIconsTopic, messages, 120*time.Second)
	if err != nil {
		return err
	}

	// going over the responses
	logging.WithField("responses", len(responses)).Info("Going over responses")
	for _, msg := range responses {
		if msg.Code != codes.Ok {
			logging.WithField("error", msg.Err).Error("Request from sync-items failed")

			continue
		}

		logging.WithField("batch", msg.ReplyToId).Info("Finished batch")
	}

	return nil
}

func (sta SyncAllItemsState) Run(in bus.Message) error {
	// validating that the provided item-ids are valid
	providedItemIds, err := blizzard.NewItemIds(in.Data)
	if err != nil {
		return err
	}
	encodedItemIds, err := providedItemIds.EncodeForDelivery()
	if err != nil {
		return err
	}

	startTime := time.Now()

	// filtering in items-to-sync
	response, err := sta.IO.BusClient.Request(sta.filterInItemsToSyncTopic, encodedItemIds, 30*time.Second)
	if err != nil {
		return err
	}

	// optionally halting
	if response.Code != codes.Ok {
		return errors.New("response code was not ok")
	}

	// parsing response data
	syncPayload, err := database.NewItemsSyncPayload(response.Data)
	if err != nil {
		return err
	}

	// handling item-ids
	if err := sta.HandleItemIds(syncPayload.Ids); err != nil {
		return err
	}

	// handling item-icons
	if err := sta.HandleItemIcons(syncPayload.IconIdsMap); err != nil {
		return err
	}

	// reporting metrics
	if err := sta.IO.BusClient.PublishMetrics(metric.Metrics{
		"sync_all_items_duration": int(int64(time.Since(startTime)) / 1000 / 1000 / 1000),
		"sync_all_items_ids":      len(syncPayload.Ids),
		"sync_all_items_icons":    len(syncPayload.IconIdsMap),
	}); err != nil {
		return err
	}

	return nil
}
