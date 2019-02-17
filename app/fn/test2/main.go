package test2

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"

	"github.com/sotah-inc/server/app/pkg/sotah"

	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
)

var projectId = os.Getenv("GCP_PROJECT")
var busClient bus.Client
var storeClient store.Client

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-test2")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}

	storeClient, err = store.NewClient(projectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return
	}
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

// 22314166

func HelloPubSub(_ context.Context, m PubSubMessage) error {
	var in bus.Message
	if err := json.Unmarshal(m.Data, &in); err != nil {
		return err
	}

	var realm sotah.Realm
	if err := json.Unmarshal([]byte(in.Data), &realm); err != nil {
		return err
	}

	aucs, err := storeClient.GetTestAuctions(realm)
	if err != nil {
		return err
	}

	msg := bus.NewMessage()
	msg.Data = strconv.Itoa(len(aucs.Auctions))
	if _, err := busClient.Publish(busClient.Topic(string(subjects.AuctionCountReceive)), msg); err != nil {
		return err
	}

	return nil
}
