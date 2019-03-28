package sync_item_icons

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/sotah-inc/server/app/pkg/util"
)

var (
	projectId = os.Getenv("GCP_PROJECT")

	busClient               bus.Client
	receiveSyncedItemsTopic *pubsub.Topic

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
	receiveSyncedItemsTopic, err = busClient.FirmTopic(string(subjects.ReceiveSyncedItems))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

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

func SyncItemIcon(payload store.IconItemsPayload) error {
	obj := itemIconsBase.GetObject(payload.Name, itemIconsBucket)
	exists, err := itemIconsBase.ObjectExists(obj)
	if err != nil {
		return err
	}
	if exists {
		logging.WithField("icon", payload.Name).Info("Item-icon already exists, skipping")

		return nil
	}

	logging.WithField("icon", payload.Name).Info("Downloading")
	respMeta, err := blizzard.Download(blizzard.DefaultGetItemIconURL(payload.Name))
	if err != nil {
		return err
	}
	if respMeta.Status != http.StatusOK {
		return errors.New("status was not OK")
	}

	// writing it out to the gcloud object
	logging.WithField("icon", payload.Name).Info("Writing to item-icons-base")
	wc := obj.NewWriter(storeClient.Context)
	wc.ContentType = "image/jpeg"
	if _, err := wc.Write(respMeta.Body); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}

	return nil
}

type HandlePayloadsJob struct {
	Err      error
	IconName string
	Ids      blizzard.ItemIds
}

func HandlePayloads(payloads store.IconItemsPayloads) (blizzard.ItemIds, error) {
	// spawning workers
	in := make(chan store.IconItemsPayload)
	out := make(chan HandlePayloadsJob)
	worker := func() {
		for payload := range in {
			if err := SyncItemIcon(payload); err != nil {
				out <- HandlePayloadsJob{
					Err:      err,
					IconName: payload.Name,
				}

				continue
			}

			out <- HandlePayloadsJob{
				Err:      nil,
				IconName: payload.Name,
				Ids:      payload.Ids,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(8, worker, postWork)

	// spinning it up
	go func() {
		for _, payload := range payloads {
			in <- payload
		}

		close(in)
	}()

	// waiting for the results to drain out
	results := blizzard.ItemIds{}
	for outJob := range out {
		if outJob.Err != nil {
			logging.WithField("error", outJob.Err.Error()).Error("Failed to sync item")

			continue
		}

		for _, id := range outJob.Ids {
			results = append(results, id)
		}
	}

	return results, nil
}

func Handle(in bus.Message) bus.Message {
	m := bus.NewMessage()

	iconIdsPayloads, err := store.NewIconItemsPayloads(in.Data)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	results, err := HandlePayloads(iconIdsPayloads)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	data, err := results.EncodeForDelivery()
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	logging.WithField("results", len(results)).Info("Received synced items payload, pushing to receive-synced-items topic")
	msg := bus.NewMessage()
	msg.Data = data
	if _, err := busClient.Publish(receiveSyncedItemsTopic, msg); err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	m.Code = codes.Ok

	return m
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func SyncItemIcons(_ context.Context, m PubSubMessage) error {
	var in bus.Message
	if err := json.Unmarshal(m.Data, &in); err != nil {
		return err
	}

	msg := Handle(in)
	msg.ReplyToId = in.ReplyToId
	if _, err := busClient.ReplyTo(in, msg); err != nil {
		return err
	}

	return nil
}
