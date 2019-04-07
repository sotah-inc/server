package cleanup_expired_manifest

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/store"
)

var (
	projectId = os.Getenv("GCP_PROJECT")

	busClient bus.Client

	storeClient store.Client

	auctionsStoreBase  store.AuctionsBaseV2
	auctionStoreBucket *storage.BucketHandle

	auctionManifestStoreBase store.AuctionManifestBaseV2
	auctionManifestBucket    *storage.BucketHandle
)

func init() {
	var err error

	busClient, err = bus.NewClient(projectId, "fn-cleanup-expired-manifest")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}

	storeClient, err = store.NewClient(projectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return
	}

	auctionsStoreBase = store.NewAuctionsBaseV2(storeClient, "us-central1")
	auctionStoreBucket, err = auctionsStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm raw-auctions bucket: %s", err.Error())

		return
	}

	auctionManifestStoreBase = store.NewAuctionManifestBaseV2(storeClient, "us-central1")
	auctionManifestBucket, err = auctionManifestStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm auction-manifest bucket: %s", err.Error())

		return
	}
}

func handleManifestCleaning(realm sotah.Realm, targetTimestamp sotah.UnixTimestamp) (int, error) {
	obj, err := auctionManifestStoreBase.GetFirmObject(targetTimestamp, realm, auctionManifestBucket)
	if err != nil {
		return 0, err
	}

	manifest, err := func() (sotah.AuctionManifest, error) {
		reader, err := obj.NewReader(storeClient.Context)
		if err != nil {
			return sotah.AuctionManifest{}, err
		}

		data, err := ioutil.ReadAll(reader)
		if err != nil {
			return sotah.AuctionManifest{}, err
		}

		var out sotah.AuctionManifest
		if err := json.Unmarshal(data, &out); err != nil {
			return sotah.AuctionManifest{}, err
		}

		return out, nil
	}()
	if err != nil {
		return 0, err
	}

	totalDeleted := 0
	for outJob := range auctionsStoreBase.DeleteAll(auctionStoreBucket, realm, manifest) {
		if outJob.Err != nil {
			return 0, outJob.Err
		}

		logging.WithFields(logrus.Fields{
			"region":           realm.Region.Name,
			"realm":            realm.Slug,
			"target-timestamp": outJob.TargetTimestamp,
		}).Info("Deleted raw-auctions object")
		totalDeleted += 1
	}

	if err := obj.Delete(storeClient.Context); err != nil {
		return 0, err
	}

	logging.WithFields(logrus.Fields{
		"region":           realm.Region.Name,
		"realm":            realm.Slug,
		"target-timestamp": targetTimestamp,
	}).Info("Deleted manifest object")

	return totalDeleted, nil
}

func Handle(job bus.CleanupAuctionManifestJob) bus.Message {
	m := bus.NewMessage()

	logging.WithFields(logrus.Fields{"job": job}).Info("Handling")

	realm := sotah.NewSkeletonRealm(blizzard.RegionName(job.RegionName), blizzard.RealmSlug(job.RealmSlug))
	targetTimestamp := sotah.UnixTimestamp(job.TargetTimestamp)

	totalDeleted, err := handleManifestCleaning(realm, targetTimestamp)
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	jobResponse := bus.CleanupAuctionManifestJobResponse{TotalDeleted: totalDeleted}
	data, err := jobResponse.EncodeForDelivery()
	if err != nil {
		m.Err = err.Error()
		m.Code = codes.GenericError

		return m
	}

	m.Data = data

	return m
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func CleanupExpiredManifest(_ context.Context, m PubSubMessage) error {
	var in bus.Message
	if err := json.Unmarshal(m.Data, &in); err != nil {
		return err
	}

	job, err := bus.NewCleanupAuctionManifestJob(in.Data)
	if err != nil {
		return err
	}

	reply := Handle(job)
	reply.ReplyToId = in.ReplyToId
	if _, err := busClient.ReplyTo(in, reply); err != nil {
		return err
	}

	return nil
}
