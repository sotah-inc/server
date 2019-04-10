package bullshit

import (
	"context"
	"log"
	"os"
	"strconv"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
)

var (
	projectId = os.Getenv("GCP_PROJECT")
	natsHost  = os.Getenv("NATS_HOST")
	natsPort  = os.Getenv("NATS_PORT")

	storeClient store.Client
	bootBase    store.BootBase
	bootBucket  *storage.BucketHandle

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

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func Welp(_ context.Context, _ PubSubMessage) error {
	matches, err := bootBase.Guard("welp.txt", "vpc-connector\n", bootBucket)
	if err != nil {
		return err
	}
	if !matches {
		logging.Info("Unmatched")

		return nil
	}

	resp, err := messengerClient.Request(string(subjects.Boot), []byte{})
	if err != nil {
		return err
	}

	logging.WithField("response", len(resp.Data)).Info("Received response")

	return nil
}
