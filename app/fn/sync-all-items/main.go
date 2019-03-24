package sync_all_items

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/sotah-inc/server/app/pkg/blizzard"

	"github.com/sotah-inc/server/app/pkg/metric"

	"cloud.google.com/go/pubsub"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

var (
	projectId = os.Getenv("GCP_PROJECT")

	busClient     bus.Client
	syncItemTopic *pubsub.Topic
)

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-sync-all-items")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}
	syncItemTopic, err = busClient.FirmTopic(string(subjects.SyncItem))
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

	// producing item-ids from bus message
	itemIds, err := blizzard.NewItemIds(in.Data)
	if err != nil {
		return err
	}

	// converting to messages for requesting
	messages := bus.NewItemIdMessages(itemIds)

	// enqueueing them and gathering result jobs
	startTime := time.Now()
	responseItems, err := busClient.BulkRequest(syncItemTopic, messages, 400*time.Second)
	if err != nil {
		return err
	}

	validatedResponseItems := bus.BulkRequestMessages{}
	for k, msg := range responseItems {
		if msg.Code != codes.Ok {
			logging.WithField("msg", msg).Error("Received erroneous response")

			continue
		}

		validatedResponseItems[k] = msg
	}

	// reporting metrics
	if err := busClient.PublishMetrics(metric.Metrics{
		"sync_all_items_duration": int(int64(time.Now().Sub(startTime)) / 1000 / 1000 / 1000),
	}); err != nil {
		return err
	}

	return nil
}
