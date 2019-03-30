package bullshit

import (
	"context"
	"log"
	"os"
	"strconv"

	"github.com/sirupsen/logrus"

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

	storeClient store.Client
	bootBase    store.BootBase
	bootBucket  *storage.BucketHandle
	itemsBase   store.ItemsBase
	itemsBucket *storage.BucketHandle

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
}

type ValidateJob struct {
	Err           error
	Id            blizzard.ItemID
	HasObjectName bool
	HasObjectURI  bool
}

func Validate(id blizzard.ItemID) ValidateJob {
	obj, err := itemsBase.GetFirmObject(id, itemsBucket)
	if err != nil {
		return ValidateJob{
			Err: err,
			Id:  id,
		}
	}

	item, err := itemsBase.NewItem(obj)
	if err != nil {
		return ValidateJob{
			Err: err,
			Id:  id,
		}
	}

	return ValidateJob{
		Err:           nil,
		Id:            id,
		HasObjectName: item.IconObjectName != "",
		HasObjectURI:  item.IconURL != "",
	}
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func Welp(_ context.Context, _ PubSubMessage) error {
	matches, err := bootBase.Guard("welp.txt", "validate-items-3\n", bootBucket)
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
	out := make(chan ValidateJob)
	worker := func() {
		for id := range in {
			out <- Validate(id)
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(32, worker, postWork)

	// enqueueing it up
	logging.Info("Queueing it up")
	go func() {
		it := itemsBucket.Objects(storeClient.Context, nil)
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
	total := 0
	missing := 0
	for job := range out {
		if job.Err != nil {
			return err
		}

		total += 1

		if !job.HasObjectURI || !job.HasObjectName {
			logging.WithFields(logrus.Fields{
				"id":              job.Id,
				"has-object-uri":  job.HasObjectURI,
				"has-object-name": job.HasObjectName,
			}).Info("Does not have both object-uri and object-name")
			missing += 1
		}
	}

	logging.WithFields(logrus.Fields{
		"total":   total,
		"missing": missing,
	}).Info("Done!")

	return nil
}
