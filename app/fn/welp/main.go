package bullshit

import (
	"context"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
)

var (
	projectId = os.Getenv("GCP_PROJECT")

	storeClient store.Client
	bootBase    store.BootBase
	bootBucket  *storage.BucketHandle

	busClient                bus.Client
	filterInItemsToSyncTopic *pubsub.Topic
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
	shit := "items-verification-2\n"
	if string(data) != shit {
		logging.Info("Unmatched")

		return nil
	}

	providedItemIds := blizzard.ItemIds{5, 1705}

	logging.WithField("item-ids", providedItemIds).Info("Filtering item-ids")

	encodedItemIds, err := providedItemIds.EncodeForDelivery()
	if err != nil {
		return err
	}

	// filtering in items-to-sync
	response, err := busClient.Request(filterInItemsToSyncTopic, encodedItemIds, 30*time.Second)
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

	logging.WithField("item-ids", itemIds).Info("Received validated item-ids to sync")

	return nil
}
