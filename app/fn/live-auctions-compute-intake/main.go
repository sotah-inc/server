package liveauctionscomputeintake

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/store"
)

var projectId = os.Getenv("GCP_PROJECT")
var busClient bus.Client

var storeClient store.Client
var liveAuctionsStoreBase store.LiveAuctionsBase
var auctionsStoreBase store.AuctionsBaseV2
var auctionsBucket *storage.BucketHandle

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-live-auctions-compute-intake")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}

	storeClient, err = store.NewClient(projectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return
	}
	liveAuctionsStoreBase = store.NewLiveAuctionsBase(storeClient)
	auctionsStoreBase = store.NewAuctionsBaseV2(storeClient)

	auctionsBucket, err = auctionsStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get auctions bucket: %s", err.Error())

		return
	}
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func LiveAuctionsComputeIntake(_ context.Context, m PubSubMessage) error {
	var in bus.Message
	if err := json.Unmarshal(m.Data, &in); err != nil {
		return err
	}

	var job bus.LoadRegionRealmTimestampsInJob
	if err := json.Unmarshal([]byte(in.Data), &job); err != nil {
		return err
	}

	region := sotah.Region{Name: blizzard.RegionName(job.RegionName)}
	realm := sotah.Realm{
		Realm:  blizzard.Realm{Slug: blizzard.RealmSlug(job.RealmSlug)},
		Region: region,
	}
	targetTime := time.Unix(int64(job.TargetTimestamp), 0)

	obj, err := auctionsStoreBase.GetFirmObject(realm, targetTime, auctionsBucket)
	if err != nil {
		return err
	}

	aucs, err := storeClient.NewAuctions(obj)
	if err != nil {
		return err
	}

	if err := liveAuctionsStoreBase.Handle(aucs, realm); err != nil {
		return err
	}

	logging.WithFields(logrus.Fields{
		"region":        region.Name,
		"realm":         realm.Slug,
		"last-modified": targetTime.Unix(),
	}).Info("Handled")

	// req := state.LiveAuctionsComputeIntakeRequest{}
	// req.RegionName = string(region.Name)
	// req.RealmSlug = string(realm.Slug)
	// jsonEncodedRequest, err := json.Marshal(req)
	// if err != nil {
	// 	return err
	// }
	//
	// topic := busClient.Topic(string(subjects.LiveAuctionsComputeIntake))
	// msg := bus.NewMessage()
	// msg.Data = string(jsonEncodedRequest)
	// if _, err := busClient.Publish(topic, msg); err != nil {
	// 	return err
	// }

	return nil
}
