package auctionscollector

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

var projectId = os.Getenv("GCP_PROJECT")
var busClient bus.Client
var regions sotah.RegionList
var regionRealms map[blizzard.RegionName]sotah.Realms
var collectAuctionsTopic *pubsub.Topic

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-auctions-collector")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}

	collectAuctionsTopic, err = busClient.FirmTopic(string(subjects.CollectAuctionsCompute))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

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
	for job := range busClient.LoadStatuses(regions) {
		if job.Err != nil {
			log.Fatalf("Failed to fetch status: %s", job.Err.Error())

			return
		}

		regionRealms[job.Region.Name] = job.Status.Realms
	}
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func AuctionsCollector(_ context.Context, m PubSubMessage) error {
	var in bus.Message
	if err := json.Unmarshal(m.Data, &in); err != nil {
		return err
	}

	for _, realms := range regionRealms {
		for _, realm := range realms {
			job := bus.CollectAuctionsJob{
				RegionName: string(realm.Region.Name),
				RealmSlug:  string(realm.Slug),
			}
			encodedJob, err := json.Marshal(job)
			if err != nil {
				return err
			}

			msg := bus.NewMessage()
			msg.Data = string(encodedJob)
			if _, err := busClient.Publish(collectAuctionsTopic, msg); err != nil {
				return err
			}

			break
		}

		break
	}

	return nil
}
