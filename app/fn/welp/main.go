package bullshit

import (
	"context"
	"log"
	"os"
	"strconv"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/sotah-inc/server/app/pkg/util"
	"google.golang.org/api/iterator"
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
	itemsCentralBucket, err = itemsCentralBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return
	}
}

type TransferJob struct {
	Id  blizzard.ItemID
	Err error
}

func Transfer(id blizzard.ItemID) TransferJob {
	src, err := itemsCentralBase.GetFirmObject(id, itemsCentralBucket)
	if err != nil {
		return TransferJob{
			Err: err,
			Id:  id,
		}
	}

	dst := itemsBase.GetObject(id, itemsBucket)
	exists, err := itemsBase.ObjectExists(dst)
	if err != nil {
		return TransferJob{
			Err: err,
			Id:  id,
		}
	}

	if exists {
		logging.WithField("item", id).Info("Item exists in destination, deleting")

		// if err := src.Delete(storeClient.Context); err != nil {
		// 	return TransferJob{
		// 		Err: err,
		// 		Id:  id,
		// 	}
		// }

		return TransferJob{
			Err: nil,
			Id:  id,
		}
	}

	logging.WithField("item", id).Info("Transferring")

	copier := dst.CopierFrom(src)
	if _, err := copier.Run(storeClient.Context); err != nil {
		return TransferJob{
			Err: err,
			Id:  id,
		}
	}

	return TransferJob{
		Err: nil,
		Id:  id,
	}
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func Welp(_ context.Context, _ PubSubMessage) error {
	matches, err := bootBase.Guard("welp.txt", "transfer-items-12\n", bootBucket)
	if err != nil {
		return err
	}
	if !matches {
		logging.Info("Unmatched")

		return nil
	}

	// spinning up workers
	logging.Info("Spinning up workers")
	in := make(chan blizzard.ItemID)
	out := make(chan TransferJob)
	worker := func() {
		for id := range in {
			out <- Transfer(id)
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(32, worker, postWork)

	// enqueueing it up
	logging.Info("Queueing it up")
	go func() {
		it := itemsCentralBucket.Objects(storeClient.Context, nil)
		for {
			objAttrs, err := it.Next()
			if err != nil {
				if err == iterator.Done {
					break
				}

				logging.WithField("error", err.Error()).Error("Failed to iterate to next")

				break
			}

			parsed, err := strconv.Atoi(objAttrs.Name[0:(len(objAttrs.Name) - len(".json.gz"))])
			if err != nil {
				logging.WithField("error", err.Error()).Error("Failed to parse name")

				break
			}

			in <- blizzard.ItemID(parsed)
		}

		close(in)
	}()

	// waiting for results to drain out
	logging.Info("Waiting for results to drain out")
	for job := range out {
		if job.Err != nil {
			return err
		}
	}

	logging.Info("Done!")

	return nil
}
