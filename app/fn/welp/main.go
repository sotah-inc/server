package bullshit

import (
	"context"
	"io/ioutil"
	"log"
	"os"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/store"
)

var (
	projectId = os.Getenv("GCP_PROJECT")

	storeClient        store.Client
	bootBase           store.BootBase
	bootBucket         *storage.BucketHandle
	itemsBase          store.ItemsBase
	itemsBucket        *storage.BucketHandle
	itemsCentralBase   store.ItemsCentralBase
	itemsCentralBucket *storage.BucketHandle

	busClient bus.Client
)

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-welp")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

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

	itemsCentralBase = store.NewItemsCentralBase(storeClient, "us-central1")
	itemsCentralBucket, err = itemsBase.GetFirmBucket()
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
	shit := "transfer-items\n"
	if string(data) != shit {
		logging.Info("Unmatched")

		return nil
	}

	return nil
}
