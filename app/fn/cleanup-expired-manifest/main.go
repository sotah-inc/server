package cleanupintake

import (
	"context"
	"encoding/json"
	"io/ioutil"
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

var storeClient store.Client
var auctionsStoreBase store.AuctionsBaseV2
var auctionManifestStoreBase store.AuctionManifestBaseV2
var auctionManifestBucket *storage.BucketHandle
var pricelistHistoriesStoreBase store.PricelistHistoriesBaseV2
var pricelistHistoriesBucket *storage.BucketHandle

func init() {
	var err error

	storeClient, err = store.NewClient(projectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return
	}
	auctionsStoreBase = store.NewAuctionsBaseV2(storeClient)

	auctionManifestStoreBase = store.NewAuctionManifestBaseV2(storeClient)
	auctionManifestBucket, err = auctionsStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm auction-manifest bucket: %s", err.Error())

		return
	}

	pricelistHistoriesStoreBase = store.NewPricelistHistoriesBaseV2(storeClient)
	pricelistHistoriesBucket, err = pricelistHistoriesStoreBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm pricelist-histories bucket: %s", err.Error())

		return
	}
}

func handleManifestCleaning(realm sotah.Realm, targetTimestamp sotah.UnixTimestamp) error {
	obj := auctionManifestStoreBase.GetObject(targetTimestamp, realm, auctionManifestBucket)
	exists, err := auctionManifestStoreBase.ObjectExists(obj)
	if err != nil {
		return err
	}
	if !exists {
		logging.Info("Auctions-manifest object does not exist, halting early")

		return nil
	}

	objAttrs, err := obj.Attrs(storeClient.Context)
	if err != nil {
		return err
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
		return err
	}

	for outJob := range auctionsStoreBase.DeleteAll(auctionsStoreBase.GetBucket(), realm, manifest) {
		if outJob.Err != nil {
			return outJob.Err
		}

		logging.WithFields(logrus.Fields{
			"region":           realm.Region.Name,
			"realm":            realm.Slug,
			"target-timestamp": outJob.TargetTimestamp,
		}).Info("Deleted raw-auctions object")
	}

	if err := obj.Delete(storeClient.Context); err != nil {
		return err
	}

	logging.WithFields(logrus.Fields{
		"region":   realm.Region.Name,
		"realm":    realm.Slug,
		"manifest": objAttrs.Name,
	}).Info("Deleted manifest object")

	return nil
}

func handlePricelistsCleaning(realm sotah.Realm, targetTimestamp sotah.UnixTimestamp) error {
	obj := pricelistHistoriesStoreBase.GetObject(time.Unix(int64(targetTimestamp), 0), realm, pricelistHistoriesBucket)
	exists, err := auctionManifestStoreBase.ObjectExists(obj)
	if err != nil {
		return err
	}
	if !exists {
		logging.Info("Pricelist-histories object does not exist, halting early")

		return nil
	}

	objAttrs, err := obj.Attrs(storeClient.Context)
	if err != nil {
		return err
	}

	if err := obj.Delete(storeClient.Context); err != nil {
		return err
	}

	logging.WithFields(logrus.Fields{
		"region":   realm.Region.Name,
		"realm":    realm.Slug,
		"manifest": objAttrs.Name,
	}).Info("Deleted pricelist-histories object")

	return nil
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func CleanupExpiredManifest(_ context.Context, m PubSubMessage) error {
	job, err := func() (bus.CleanupAuctionManifestJob, error) {
		var in bus.Message
		if err := json.Unmarshal(m.Data, &in); err != nil {
			return bus.CleanupAuctionManifestJob{}, err
		}

		var out bus.CleanupAuctionManifestJob
		if err := json.Unmarshal([]byte(in.Data), &out); err != nil {
			return bus.CleanupAuctionManifestJob{}, err
		}

		return out, nil
	}()
	if err != nil {
		return err
	}

	logging.WithFields(logrus.Fields{"job": job}).Info("Handling")

	realm := sotah.Realm{
		Realm:  blizzard.Realm{Slug: blizzard.RealmSlug(job.RealmSlug)},
		Region: sotah.Region{Name: blizzard.RegionName(job.RegionName)},
	}
	targetTimestamp := sotah.UnixTimestamp(job.TargetTimestamp)

	if err := handleManifestCleaning(realm, targetTimestamp); err != nil {
		return err
	}

	if err := handlePricelistsCleaning(realm, targetTimestamp); err != nil {
		return err
	}

	return nil
}
