package cleanupintake

import (
	"context"
	"log"
	"os"

	"github.com/sotah-inc/server/app/pkg/store"
)

var projectId = os.Getenv("GCP_PROJECT")

var storeClient store.Client
var auctionsStoreBase store.AuctionsBase

func init() {
	var err error

	storeClient, err = store.NewClient(projectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return
	}
	auctionsStoreBase = store.NewAuctionsBase(storeClient)
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func CleanupIntake(_ context.Context, m PubSubMessage) error {
	return nil
}
