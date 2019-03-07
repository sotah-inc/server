package bullshit

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

var projectId = os.Getenv("GCP_PROJECT")

var regionRealms map[blizzard.RegionName]sotah.Realms

var busClient bus.Client
var cleanupTopic *pubsub.Topic

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-cleanup")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}
	cleanupTopic, err = busClient.FirmTopic(string(subjects.BullshitIntake))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return
	}

	bootResponse, err := func() (state.BootResponse, error) {
		msg, err := busClient.RequestFromTopic(string(subjects.Boot), "", 5*time.Second)
		if err != nil {
			return state.BootResponse{}, err
		}

		var out state.BootResponse
		if err := json.Unmarshal([]byte(msg.Data), &out); err != nil {
			return state.BootResponse{}, err
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

		realms := sotah.Realms{}
		for _, realm := range job.Status.Realms {
			realms = append(realms, realm)
		}

		regionRealms[job.Region.Name] = realms
	}
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func Bullshit(_ context.Context, _ PubSubMessage) error {
	for job := range busClient.LoadRegionRealms(cleanupTopic, regionRealms) {
		if job.Err != nil {
			logging.WithFields(logrus.Fields{
				"error":  job.Err.Error(),
				"region": job.Realm.Region.Name,
				"realm":  job.Realm.Slug,
			}).Error("Failed to queue message")

			continue
		}

		logging.WithFields(logrus.Fields{
			"region": job.Realm.Region.Name,
			"realm":  job.Realm.Slug,
		}).Info("Queued up realm")
	}

	return nil
}
