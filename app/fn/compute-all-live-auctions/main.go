package compute_all_live_auctions

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"cloud.google.com/go/pubsub"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

var projectId = os.Getenv("GCP_PROJECT")

var busClient bus.Client
var computeLiveAuctionsTopic *pubsub.Topic

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-compute-all-live-auctions")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}
	computeLiveAuctionsTopic, err = busClient.FirmTopic(string(subjects.ComputeLiveAuctions))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return
	}
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func ComputeAllLiveAuctions(_ context.Context, m PubSubMessage) error {
	var in bus.Message
	if err := json.Unmarshal(m.Data, &in); err != nil {
		return err
	}

	tuple, err := bus.NewRegionRealmTimestampTuples(in.Data)
	if err != nil {
		return err
	}

	logging.WithField("tuple", tuple).Info("Received")

	return nil
}
