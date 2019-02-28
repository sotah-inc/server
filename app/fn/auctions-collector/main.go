package auctionscollector

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/sotah-inc/server/app/pkg/blizzard"

	"github.com/sotah-inc/server/app/pkg/sotah"

	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

var projectId = os.Getenv("GCP_PROJECT")
var busClient bus.Client
var regions sotah.RegionList
var regionRealms map[blizzard.RegionName]sotah.Realms

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

	var bootResponse state.BootResponse
	if err := json.Unmarshal([]byte(msg.Data), &bootResponse); err != nil {
		log.Fatalf("Failed to unmarshal boot data: %s", err.Error())

		return
	}
	regions = bootResponse.Regions

	regionRealms = map[blizzard.RegionName]sotah.Realms{}
	for _, reg := range regions {
		status, err := busClient.NewStatus(reg)
		if err != nil {
			log.Fatalf("Failed to fetch status: %s", err.Error())

			return
		}

		regionRealms[reg.Name] = status.Realms
	}
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func AuctionsCollector(_ context.Context, m PubSubMessage) error {
	realmCount := 0
	for _, realms := range regionRealms {
		realmCount += len(realms)
	}
	logging.WithField("realms", realmCount).Info("Received request")

	return nil
}
