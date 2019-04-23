package bullshit

import (
	"context"
	"log"
	"os"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah/gameversions"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/sotah-inc/server/app/pkg/store/regions"
	"github.com/sotah-inc/server/app/pkg/util"
	"google.golang.org/api/iterator"
)

var (
	projectId = os.Getenv("GCP_PROJECT")

	storeClient     store.Client
	bootBase        store.BootBase
	bootBucket      *storage.BucketHandle
	itemIconsBase   store.ItemIconsBase
	itemIconsBucket *storage.BucketHandle
)

func init() {
	var err error

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

	itemIconsBase = store.NewItemIconsBase(storeClient, regions.USCentral1, gameversions.Retail)
	itemIconsBucket, err = itemIconsBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return
	}
}

func MakePublic(name string) error {
	obj := itemIconsBucket.Object(name)

	// setting acl of item-icon object to public
	acl := obj.ACL()
	if err := acl.Set(storeClient.Context, storage.AllUsers, storage.RoleReader); err != nil {
		return err
	}

	return nil
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

type MakePublicJob struct {
	Err  error
	Name string
}

func Welp(_ context.Context, _ PubSubMessage) error {
	matches, err := bootBase.Guard("welp.txt", "item-icons-6\n", bootBucket)
	if err != nil {
		logging.WithField("error", err.Error()).Error("Failed to check boot-base guard")

		return err
	}
	if !matches {
		logging.Info("Unmatched")

		return nil
	}

	// spawning workers
	in := make(chan string)
	out := make(chan MakePublicJob)
	worker := func() {
		for name := range in {
			if err := MakePublic(name); err != nil {
				out <- MakePublicJob{
					Err:  err,
					Name: name,
				}

				continue
			}

			out <- MakePublicJob{
				Err:  nil,
				Name: name,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(8, worker, postWork)

	// spinning it up
	go func() {
		it := itemIconsBucket.Objects(storeClient.Context, nil)
		for {
			objAttrs, err := it.Next()
			if err != nil {
				if err == iterator.Done {
					break
				}

				logging.WithField("error", err.Error()).Fatal("Failed to iterate")

				break
			}

			in <- objAttrs.Name
		}

		close(in)
	}()

	// waiting for results to drain out
	for job := range out {
		if job.Err != nil {
			return job.Err
		}
	}

	logging.Info("Completed")

	return nil
}
