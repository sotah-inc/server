package sync_items

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
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/sotah-inc/server/app/pkg/util"
)

var (
	projectId = os.Getenv("GCP_PROJECT")

	busClient               bus.Client
	receiveSyncedItemsTopic *pubsub.Topic

	primaryRegion sotah.Region

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

	bootBase := store.NewBootBase(storeClient, "us-central1")

	var bootBucket *storage.BucketHandle
	bootBucket, err = bootBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return
	}

	blizzardCredentials, err := bootBase.GetBlizzardCredentials(bootBucket)
	if err != nil {
		log.Fatalf("Failed to get blizzard-credentials: %s", err.Error())

		return
	}
	blizzardClient, err = blizzard.NewClient(blizzardCredentials.ClientId, blizzardCredentials.ClientSecret)
	if err != nil {
		log.Fatalf("Failed to create blizzard client: %s", err.Error())

		return
	}

	regions, err := bootBase.GetRegions(bootBucket)
	if err != nil {
		log.Fatalf("Failed to get regions: %s", err.Error())

		return
	}
	primaryRegion, err = regions.GetPrimaryRegion()
	if err != nil {
		log.Fatalf("Failed to get primary region: %s", err.Error())

		return
	}
}

func SyncItem(id blizzard.ItemID) error {
	exists, err := itemsBase.ObjectExists(id, itemsBucket)
	if err != nil {
		return err
	}
	if exists {
		logging.WithField("id", id).Info("Item already exists, skipping")

		return nil
	}

	logging.WithField("id", id).Info("Downloading")
	uri, err := blizzardClient.AppendAccessToken(blizzard.DefaultGetItemURL(primaryRegion.Hostname, id))
	if err != nil {
		return err
	}

	respMeta, err := blizzard.Download(uri)
	if err != nil {
		return err
	}
	if respMeta.Status != http.StatusOK {
		return errors.New("status was not OK")
	}

	logging.WithField("id", id).Info("Parsing and encoding")
	item, err := blizzard.NewItem(respMeta.Body)
	if err != nil {
		return err
	}

	jsonEncoded, err := json.Marshal(item)
	if err != nil {
		return err
	}

	gzipEncodedBody, err := util.GzipEncode(jsonEncoded)
	if err != nil {
		return err
	}

	// writing it out to the gcloud object
	logging.WithField("id", id).Info("Writing to items-base")
	wc := itemsBase.GetObject(id, itemsBucket).NewWriter(storeClient.Context)
	wc.ContentType = "application/json"
	wc.ContentEncoding = "gzip"
	if _, err := wc.Write(gzipEncodedBody); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}

	return nil
}

type HandleIdsJob struct {
	Err error
	Id  blizzard.ItemID
}

func HandleIds(ids blizzard.ItemIds) (blizzard.ItemIds, error) {
	// spawning workers
	in := make(chan blizzard.ItemID)
	out := make(chan HandleIdsJob)
	worker := func() {
		for id := range in {
			if err := SyncItem(id); err != nil {
				out <- HandleIdsJob{
					Err: err,
					Id:  id,
				}

				continue
			}

			out <- HandleIdsJob{
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
			logging.WithField("error", outJob.Err.Error()).Error("Failed to sync item")

			continue
		}

		results = append(results, outJob.Id)
	}

	return results, nil
}

func Handle(in bus.Message) bus.Message {
	m := bus.NewMessage()

	itemIds, err := blizzard.NewItemIds(in.Data)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	results, err := HandleIds(itemIds)
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

func SyncItems(_ context.Context, m PubSubMessage) error {
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
