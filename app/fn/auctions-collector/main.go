package auctionscollector

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
	"github.com/sotah-inc/server/app/pkg/store"
)

var projectId = os.Getenv("GCP_PROJECT")

var regionRealms map[blizzard.RegionName]sotah.Realms

var busClient bus.Client
var collectAuctionsTopic *pubsub.Topic

var storeClient store.Client
var auctionManifestStoreBase store.AuctionManifestBase

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

	storeClient, err = store.NewClient(projectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return
	}
	auctionManifestStoreBase = store.NewAuctionManifestBase(storeClient)

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

func AuctionsCollector(_ context.Context, m PubSubMessage) error {
	var in bus.Message
	if err := json.Unmarshal(m.Data, &in); err != nil {
		return err
	}

	for job := range busClient.LoadRegionRealms(collectAuctionsTopic, regionRealms) {
		if job.Err != nil {
			logging.WithFields(logrus.Fields{
				"error":  job.Err.Error(),
				"region": job.Realm.Region.Name,
				"realm":  job.Realm.Slug,
			}).Error("Failed to queue message")

			continue
		}
	}

	for job := range auctionManifestStoreBase.DeleteAll(regionRealms) {
		if job.Err != nil {
			return job.Err
		}

		logging.WithFields(logrus.Fields{
			"region": job.Realm.Region.Name,
			"realm":  job.Realm.Slug,
		}).Info("Cleared auction-manifest bucket")
	}

	return nil
}
