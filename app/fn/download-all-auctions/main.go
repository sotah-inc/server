package download_all_auctions

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
)

var (
	projectId = os.Getenv("GCP_PROJECT")

	regionRealms map[blizzard.RegionName]sotah.Realms

	busClient                               bus.Client
	downloadAuctionsTopic                   *pubsub.Topic
	syncAllItemsTopic                       *pubsub.Topic
	receivedComputedLiveAuctionsTopic       *pubsub.Topic
	receivedComputedPricelistHistoriesTopic *pubsub.Topic
)

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-download-all-auctions")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}
	downloadAuctionsTopic, err = busClient.FirmTopic(string(subjects.DownloadAuctions))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return
	}
	syncAllItemsTopic, err = busClient.FirmTopic(string(subjects.SyncAllItems))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return
	}
	receivedComputedLiveAuctionsTopic, err = busClient.FirmTopic(string(subjects.ReceiveComputedLiveAuctions))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return
	}
	receivedComputedPricelistHistoriesTopic, err = busClient.FirmTopic(string(subjects.ReceiveComputedPricelistHistories))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return
	}

	storeClient, err := store.NewClient(projectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return
	}

	bootBase := store.NewBootBase(storeClient, "us-central1")
	var bootBucket *storage.BucketHandle
	bootBucket, err = bootBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return
	}
	regionRealms, err = bootBase.GetRegionRealms(bootBucket)
	if err != nil {
		log.Fatalf("Failed to get region-realms: %s", err.Error())

		return
	}
}

func PublishToSyncItems(tuples bus.RegionRealmTimestampTuples) error {
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
	if _, err := busClient.Publish(syncAllItemsTopic, msg); err != nil {
		return err
	}

	return nil
}

func PublishToReceiveComputedLiveAuctions(tuples bus.RegionRealmTimestampTuples) error {
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
	if _, err := busClient.Publish(receivedComputedLiveAuctionsTopic, msg); err != nil {
		return err
	}

	return nil
}

func PublishToReceivePricelistHistories(tuples bus.RegionRealmTimestampTuples) error {
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
	if _, err := busClient.Publish(receivedComputedPricelistHistoriesTopic, msg); err != nil {
		return err
	}

	return nil
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func DownloadAllAuctions(_ context.Context, _ PubSubMessage) error {
	// producing messages
	logging.Info("Producing messages for bulk requesting")
	messages, err := bus.NewCollectAuctionMessages(regionRealms)
	if err != nil {
		return err
	}

	// enqueueing them and gathering result jobs
	startTime := time.Now()
	responseItems, err := busClient.BulkRequest(downloadAuctionsTopic, messages, 400*time.Second)
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
	if err := busClient.PublishMetrics(metric.Metrics{
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
	if err := PublishToSyncItems(tuples); err != nil {
		return err
	}

	// publishing to receive-computed-live-auctions
	if err := PublishToReceiveComputedLiveAuctions(tuples); err != nil {
		return err
	}

	// publishing to receive-live-auctions
	if err := PublishToReceivePricelistHistories(tuples); err != nil {
		return err
	}

	return nil
}
