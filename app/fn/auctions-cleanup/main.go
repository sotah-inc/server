package auctionscleanup

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/logging"

	"cloud.google.com/go/pubsub"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

var projectId = os.Getenv("GCP_PROJECT")

var regionRealms map[blizzard.RegionName]sotah.Realms

var busClient bus.Client
var auctionsCleanupTopic *pubsub.Topic

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-auctions-collector")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}
	auctionsCleanupTopic, err = busClient.FirmTopic(string(subjects.AuctionsCleanupCompute))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return
	}

	bootResponse, err := func() (state.AuthenticatedBootResponse, error) {
		msg, err := busClient.RequestFromTopic(string(subjects.Boot), "", 5*time.Second)
		if err != nil {
			return state.AuthenticatedBootResponse{}, err
		}

		var out state.AuthenticatedBootResponse
		if err := json.Unmarshal([]byte(msg.Data), &out); err != nil {
			return state.AuthenticatedBootResponse{}, err
		}

		return out, nil
	}()
	if err != nil {
		log.Fatalf("Failed to get authenticated-boot-response: %s", err.Error())

		return
	}

	regions := bootResponse.Regions

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

func AuctionsCleanup(_ context.Context, _ PubSubMessage) error {
	priorManifestTimestamps := []sotah.UnixTimestamp{}
	normalizedTime := sotah.NormalizeTargetDate(time.Now())
	for i := 1; i < 2; i++ {
		then := normalizedTime.AddDate(0, 0, -1*i)

		priorManifestTimestamps = append(priorManifestTimestamps, sotah.UnixTimestamp(then.Unix()))
	}

	for _, realms := range regionRealms {
		for _, realm := range realms {
			if realm.Region.Name != "us" {
				continue
			}

			if realm.Slug != "earthen-ring" {
				continue
			}

			for expiredManifestTimestamp := range priorManifestTimestamps {
				logging.WithFields(logrus.Fields{
					"region":   realm.Region.Name,
					"realm":    realm.Slug,
					"manifest": expiredManifestTimestamp,
				}).Info("Queueing for deletion")
			}
		}
	}

	return nil
}
