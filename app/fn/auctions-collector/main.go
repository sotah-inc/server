package auctionscollector

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/sotah-inc/server/app/pkg/logging"

	"github.com/sotah-inc/server/app/pkg/state"

	"github.com/sotah-inc/server/app/pkg/state/subjects"

	"github.com/sotah-inc/server/app/pkg/bus"
)

var projectId = os.Getenv("GCP_PROJECT")
var busClient bus.Client
var bootResponse state.BootResponse

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-auctions-collector")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}

	msg, err := busClient.RequestFromTopic(string(subjects.Boot), "", 5*time.Second)
	if err != nil {
		log.Fatalf("Failed to request boot data: %s", err.Error())

		return
	}

	if err := json.Unmarshal([]byte(msg.Data), &bootResponse); err != nil {
		log.Fatalf("Failed to unmarshal boot data: %s", err.Error())

		return
	}
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func LiveAuctionsComputeIntake(_ context.Context, m PubSubMessage) error {
	logging.WithField("regions", len(bootResponse.Regions)).Info("Received request")

	return nil
}
