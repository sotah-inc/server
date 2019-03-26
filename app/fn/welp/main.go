package bullshit

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

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

	primaryRegion sotah.Region

	blizzardClient blizzard.Client

	storeClient store.Client
	bootBase    store.BootBase
	bootBucket  *storage.BucketHandle
	itemsBase   store.ItemsBase
	itemsBucket *storage.BucketHandle

	busClient                bus.Client
	filterInItemsToSyncTopic *pubsub.Topic
	receiveSyncedItemsTopic  *pubsub.Topic
)

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-welp")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}
	filterInItemsToSyncTopic, err = busClient.FirmTopic(string(subjects.FilterInItemsToSync))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

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

	bootBase = store.NewBootBase(storeClient, "us-central1")
	bootBucket, err = bootBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return
	}

	itemsBase = store.NewItemsBase(storeClient, "us-central1")
	itemsBucket, err = itemsBase.GetFirmBucket()
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

	logging.WithField("id", id).Info("Writing to items-base")
	// writing it out to the gcloud object
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

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func Welp(_ context.Context, _ PubSubMessage) error {
	obj, err := bootBase.GetFirmObject("welp.txt", bootBucket)
	if err != nil {
		return err
	}
	reader, err := obj.NewReader(storeClient.Context)
	if err != nil {
		return err
	}
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	shit := "items-verification-5\n"
	if string(data) != shit {
		logging.Info("Unmatched")

		return nil
	}

	providedItemIds := blizzard.ItemIds{45468}

	logging.WithField("item-ids", providedItemIds).Info("Filtering item-ids")

	encodedItemIds, err := providedItemIds.EncodeForDelivery()
	if err != nil {
		return err
	}

	// filtering in items-to-sync
	resp, err := busClient.Request(filterInItemsToSyncTopic, encodedItemIds, 30*time.Second)
	if err != nil {
		return err
	}

	// optionally halting
	if resp.Code != codes.Ok {
		return errors.New("resp code was not ok")
	}

	// parsing resp data
	filteredItemIds, err := blizzard.NewItemIds(resp.Data)
	if err != nil {
		return err
	}

	if len(filteredItemIds) == 0 {
		logging.Info("No item-ids returned, skipping")

		return nil
	}

	logging.WithField("item-ids", filteredItemIds).Info("Received validated item-ids to sync, attempting to sync")

	syncedItemIds := blizzard.ItemIds{}
	for _, id := range filteredItemIds {
		if err := SyncItem(id); err != nil {
			return err
		}

		syncedItemIds = append(syncedItemIds, id)
	}

	payload, err := syncedItemIds.EncodeForDelivery()
	if err != nil {
		return err
	}

	logging.Info("Received synced items payload, pushing to receive-synced-items topic")
	msg := bus.NewMessage()
	msg.Data = payload
	if _, err := busClient.Publish(receiveSyncedItemsTopic, msg); err != nil {
		return err
	}

	return nil
}
