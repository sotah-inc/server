package bullshit

import (
	"context"
	"log"
	"os"
	"strconv"

	"google.golang.org/api/iterator"

	"github.com/sotah-inc/server/app/pkg/util"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/sotah/gameversions"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/sotah-inc/server/app/pkg/store/regions"
)

var (
	projectId = os.Getenv("GCP_PROJECT")
	natsHost  = os.Getenv("NATS_HOST")
	natsPort  = os.Getenv("NATS_PORT")

	storeClient     store.Client
	bootBase        store.BootBase
	bootBucket      *storage.BucketHandle
	itemIconsBase   store.ItemIconsBase
	itemIconsBucket *storage.BucketHandle

	messengerClient messenger.Messenger
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

	var parsedNatsPort int
	parsedNatsPort, err = strconv.Atoi(natsPort)
	if err != nil {
		log.Fatalf("Failed to parse nats port: %s", err.Error())

		return
	}

	messengerClient, err = messenger.NewMessenger(natsHost, parsedNatsPort)
	if err != nil {
		log.Fatalf("Failed to get messenger client: %s", err.Error())

		return
	}
}

func MakePublic(name string) error {
	obj, err := itemIconsBase.GetFirmObject(name, itemIconsBucket)
	if err != nil {
		return err
	}

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
	matches, err := bootBase.Guard("welp.txt", "item-icons\n", bootBucket)
	if err != nil {
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
