package sync_items

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/sotah-inc/server/app/pkg/util"
)

var (
	projectId = os.Getenv("GCP_PROJECT")

	busClient               bus.Client
	receiveSyncedItemsTopic *pubsub.Topic

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

func SyncItem(id blizzard.ItemID) error {
	return nil
}

type HandleJob struct {
	Err error
	Id  blizzard.ItemID
}

func Handle(ids blizzard.ItemIds) (blizzard.ItemIds, error) {
	// spawning workers
	in := make(chan blizzard.ItemID)
	out := make(chan HandleJob)
	worker := func() {
		for id := range in {
			if err := SyncItem(id); err != nil {
				out <- HandleJob{
					Err: err,
					Id:  id,
				}

				continue
			}

			out <- HandleJob{
				Err: nil,
				Id:  id,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(8, worker, postWork)

	// spinning it up
	go func() {
		for _, id := range ids {
			in <- id
		}

		close(in)
	}()

	// waiting for the results to drain out
	results := blizzard.ItemIds{}
	for outJob := range out {
		if outJob.Err != nil {
			return blizzard.ItemIds{}, outJob.Err
		}

		results = append(results, outJob.Id)
	}

	return results, nil
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func SyncItems(_ context.Context, m PubSubMessage) error {
	var in bus.Message
	if err := json.Unmarshal(m.Data, &in); err != nil {
		return err
	}

	itemIds, err := blizzard.NewItemIds(in.Data)
	if err != nil {
		return err
	}

	results, err := Handle(itemIds)
	if err != nil {
		return err
	}

	data, err := results.EncodeForDelivery()
	if err != nil {
		return err
	}

	msg := bus.NewMessage()
	msg.Data = data
	if _, err := busClient.Publish(receiveSyncedItemsTopic, msg); err != nil {
		return err
	}

	return nil
}
