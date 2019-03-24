package sync_item

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"

	"github.com/sotah-inc/server/app/pkg/logging"

	"cloud.google.com/go/storage"

	"github.com/sotah-inc/server/app/pkg/store"

	"github.com/sotah-inc/server/app/pkg/blizzard"

	"github.com/sotah-inc/server/app/pkg/bus"
)

var (
	projectId = os.Getenv("GCP_PROJECT")

	busClient bus.Client

	blizzardClient blizzard.Client

	storeClient     store.Client
	itemsBase       store.ItemsBase
	itemsBucket     *storage.BucketHandle
	itemIconsBase   store.ItemIconsBase
	itemIconsBucket *storage.BucketHandle
)

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-sync-all-items")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}

	storeClient, err = store.NewClient(projectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return
	}

	itemsBase = store.NewItemsBase(storeClient, "us-central1")
	itemsBucket, err = itemsBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return
	}

	itemIconsBase = store.NewItemIconsBase(storeClient, "us-central1")
	itemIconsBucket, err = itemIconsBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return
	}
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func SyncItem(_ context.Context, m PubSubMessage) error {
	var in bus.Message
	if err := json.Unmarshal(m.Data, &in); err != nil {
		return err
	}

	parsedId, err := strconv.Atoi(string(in.Data))
	if err != nil {
		return err
	}

	itemId := blizzard.ItemID(parsedId)
	exists, err := itemsBase.ObjectExists(itemId, itemsBucket)
	if err != nil {
		return err
	}

	if exists {
		logging.WithField("id", itemId).Info("Item exists")
	} else {
		logging.WithField("id", itemId).Info("Item does not exists")
	}

	return nil
}
