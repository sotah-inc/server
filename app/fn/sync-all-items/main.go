package sync_all_items

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

var (
	projectId = os.Getenv("GCP_PROJECT")

	busClient                bus.Client
	syncItemsTopic           *pubsub.Topic
	filterInItemsToSyncTopic *pubsub.Topic
)

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-sync-all-items")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}
	syncItemsTopic, err = busClient.FirmTopic(string(subjects.SyncItems))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return
	}
	filterInItemsToSyncTopic, err = busClient.FirmTopic(string(subjects.FilterInItemsToSync))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return
	}
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func SyncAllItems(_ context.Context, m PubSubMessage) error {
	var in bus.Message
	if err := json.Unmarshal(m.Data, &in); err != nil {
		return err
	}

	startTime := time.Now()

	// filtering in items-to-sync
	response, err := busClient.Request(filterInItemsToSyncTopic, in.Data, 30*time.Second)
	if err != nil {
		return err
	}

	// optionally halting
	if response.Code != codes.Ok {
		return errors.New("response code was not ok")
	}

	// parsing response data
	itemIds, err := blizzard.NewItemIds(response.Data)
	if err != nil {
		return err
	}

	// batching items together
	batchSize := 1000
	itemIdsBatches := map[int]blizzard.ItemIds{}
	for i, id := range itemIds {
		key := (i - (i % batchSize)) / batchSize
		batch := func() blizzard.ItemIds {
			out, ok := itemIdsBatches[key]
			if !ok {
				return blizzard.ItemIds{}
			}

			return out
		}()
		batch = append(batch, id)

		itemIdsBatches[key] = batch
	}

	logging.WithFields(logrus.Fields{
		"ids":     len(itemIds),
		"batches": len(itemIdsBatches),
	}).Info("Enqueueing batches")

	for _, batch := range itemIdsBatches {
		data, err := batch.EncodeForDelivery()
		if err != nil {
			return err
		}

		msg := bus.NewMessage()
		msg.Data = data
		if _, err := busClient.Publish(syncItemsTopic, msg); err != nil {
			return err
		}
	}

	// reporting metrics
	if err := busClient.PublishMetrics(metric.Metrics{
		"sync_all_items_duration": int(int64(time.Now().Sub(startTime)) / 1000 / 1000 / 1000),
	}); err != nil {
		return err
	}

	return nil
}
