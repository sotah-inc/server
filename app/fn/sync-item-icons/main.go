package sync_item_icons

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

	storeClient         store.Client
	itemsBase           store.ItemsBase
	itemsBucket         *storage.BucketHandle
	itemIconsBase       store.ItemIconsBase
	itemIconsBucket     *storage.BucketHandle
	itemIconsBucketName string
)

const storeItemIconURLFormat = "https://storage.googleapis.com/%s/%s"

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

	bktAttrs, err := itemIconsBucket.Attrs(storeClient.Context)
	if err != nil {
		log.Fatalf("Failed to get bucket attrs: %s", err.Error())

		return
	}

	itemIconsBucketName = bktAttrs.Name
}

func UpdateItem(id blizzard.ItemID, objectUri string, objectName string) error {
	// gathering the object
	obj, err := itemsBase.GetFirmObject(id, itemsBucket)
	if err != nil {
		return err
	}

	// reading the item from the object
	item, err := itemsBase.NewItem(obj)
	if err != nil {
		return err
	}

	// updating the icon data with the object uri (for non-cdn usage) and the object name (for cdn usage)
	item.IconURL = objectUri
	item.IconObjectName = objectName

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

func UpdateItems(objectUri string, objectName string, ids blizzard.ItemIds) error {
	// spawning workers
	in := make(chan blizzard.ItemID)
	out := make(chan error)
	worker := func() {
		for id := range in {
			if err := UpdateItem(id, objectUri, objectName); err != nil {
				out <- err

				continue
			}

			out <- nil
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
	for err := range out {
		if err != nil {
			logging.WithField("error", err.Error()).Error("Failed to update items with icon data")

			continue
		}
	}

	return nil
}

func SyncExistingItemIcon(payload sotah.IconItemsPayload) error {
	obj, err := itemIconsBase.GetFirmObject(payload.Name, itemIconsBucket)
	if err != nil {
		return err
	}

	// gathering obj attrs for generating a valid uri
	objAttrs, err := obj.Attrs(storeClient.Context)
	if err != nil {
		return err
	}

	objectUri := fmt.Sprintf(storeItemIconURLFormat, itemIconsBucketName, objAttrs.Name)

	return UpdateItems(objectUri, objAttrs.Name, payload.Ids)
}

func SyncItemIcon(payload sotah.IconItemsPayload) error {
	obj := itemIconsBase.GetObject(payload.Name, itemIconsBucket)
	exists, err := itemIconsBase.ObjectExists(obj)
	if err != nil {
		return err
	}
	if exists {
		logging.WithField("icon", payload.Name).Info("Item-icon already exists, updating items")

		return SyncExistingItemIcon(payload)
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

	// setting acl of item-icon object to public
	acl := obj.ACL()
	if err := acl.Set(storeClient.Context, storage.AllUsers, storage.RoleReader); err != nil {
		return err
	}

	// gathering obj attrs for generating a valid uri
	objAttrs, err := obj.Attrs(storeClient.Context)
	if err != nil {
		return err
	}

	objectUri := fmt.Sprintf(storeItemIconURLFormat, itemIconsBucketName, objAttrs.Name)

	return UpdateItems(objectUri, objAttrs.Name, payload.Ids)
}

type HandlePayloadsJob struct {
	Err      error
	IconName string
	Ids      blizzard.ItemIds
}

func HandlePayloads(payloads sotah.IconItemsPayloads) (blizzard.ItemIds, error) {
	// spawning workers
	in := make(chan sotah.IconItemsPayload)
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

			return blizzard.ItemIds{}, outJob.Err
		}

		for _, id := range outJob.Ids {
			results = append(results, id)
		}
	}

	return results, nil
}

func Handle(in bus.Message) bus.Message {
	m := bus.NewMessage()

	iconIdsPayloads, err := sotah.NewIconItemsPayloads(in.Data)
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
