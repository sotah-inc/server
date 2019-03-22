package bullshit_intake

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/twinj/uuid"
	"google.golang.org/api/iterator"
)

var (
	projectId = os.Getenv("GCP_PROJECT")

	storeClient store.Client

	auctionManifestStoreBaseV2 store.AuctionManifestBaseV2
	manifestBucket             *storage.BucketHandle

	auctionsStoreBaseV2 store.AuctionsBaseV2
	rawAuctionsBucket   *storage.BucketHandle

	bootBase   store.BootBase
	bootBucket *storage.BucketHandle

	pricelistHistoriesBase   store.PricelistHistoriesBaseV2
	pricelistHistoriesBucket *storage.BucketHandle

	busClient            bus.Client
	auctionsCleanupTopic *pubsub.Topic
)

func init() {
	var err error

	busClient, err = bus.NewClient(projectId, "fn-bullshit-intake")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}
	auctionsCleanupTopic, err = busClient.FirmTopic(string(subjects.CleanupExpiredManifest))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return
	}

	storeClient, err = store.NewClient(projectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return
	}

	auctionsStoreBaseV2 = store.NewAuctionsBaseV2(storeClient, "us-central1")
	rawAuctionsBucket, err = auctionsStoreBaseV2.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return
	}

	auctionManifestStoreBaseV2 = store.NewAuctionManifestBaseV2(storeClient, "us-central1")
	manifestBucket, err = auctionManifestStoreBaseV2.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return
	}

	bootBase = store.NewBootBase(storeClient, "us-central1")
	bootBucket, err = bootBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return
	}

	pricelistHistoriesBase = store.NewPricelistHistoriesBaseV2(storeClient, "us-central1")
	pricelistHistoriesBucket, err = pricelistHistoriesBase.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get firm bucket: %s", err.Error())

		return
	}
}

func RebuildManifest(realm sotah.Realm) error {
	currentNormalizedTime := sotah.NormalizeTargetDate(time.Now())

	manifests := map[sotah.UnixTimestamp]sotah.AuctionManifest{}

	it := rawAuctionsBucket.Objects(
		storeClient.Context,
		&storage.Query{
			Delimiter: "/",
			Prefix:    fmt.Sprintf("%s/%s/", realm.Region.Name, realm.Slug),
		},
	)
	for {
		objAttrs, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}

			if err != nil {
				return err
			}
		}

		parts := strings.Split(objAttrs.Name, ".")
		if _, err := strconv.Atoi(parts[0]); err != nil {
			return err
		}

		objTimestamp, err := strconv.Atoi(parts[0])
		if err != nil {
			return err
		}

		normalizedTime := sotah.NormalizeTargetDate(time.Unix(int64(objTimestamp), 0))

		if normalizedTime.After(currentNormalizedTime) {
			continue
		}

		normalizedTimestamp := sotah.UnixTimestamp(normalizedTime.Unix())
		nextManifest := func() sotah.AuctionManifest {
			result, ok := manifests[normalizedTimestamp]
			if !ok {
				return sotah.AuctionManifest{}
			}

			return result
		}()
		manifests[normalizedTimestamp] = append(nextManifest, sotah.UnixTimestamp(objTimestamp))
	}

	for writeAllOutJob := range auctionManifestStoreBaseV2.WriteAll(manifestBucket, realm, manifests) {
		if writeAllOutJob.Err != nil {
			return writeAllOutJob.Err
		}
	}

	return nil
}

func CheckManifestForExpired(realm sotah.Realm) error {
	limit := sotah.NormalizeTargetDate(time.Now()).AddDate(0, 0, -14)
	prefix := fmt.Sprintf("%s/%s/", realm.Region.Name, realm.Slug)
	it := manifestBucket.Objects(storeClient.Context, &storage.Query{Prefix: prefix})
	for {
		objAttrs, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}

			if err != nil {
				return err
			}
		}

		targetTimestamp, err := strconv.Atoi(objAttrs.Name[len(prefix):(len(objAttrs.Name) - len(".json"))])
		if err != nil {
			return err
		}

		targetTime := time.Unix(int64(targetTimestamp), 0)
		if targetTime.After(limit) {
			continue
		}

		logging.WithFields(logrus.Fields{
			"region":           realm.Region.Name,
			"realm":            realm.Slug,
			"target-timestamp": targetTimestamp,
		}).Info("Found expired, enqueueing")

		jsonEncoded, err := json.Marshal(bus.CleanupAuctionManifestJob{
			RegionName:      string(realm.Region.Name),
			RealmSlug:       string(realm.Slug),
			TargetTimestamp: int(targetTime.Unix()),
		})
		if err != nil {
			return err
		}

		msg := bus.NewMessage()
		msg.Data = string(jsonEncoded)

		if _, err := busClient.Publish(auctionsCleanupTopic, msg); err != nil {
			return err
		}
	}

	return nil
}

func SetMissingVersion(realm sotah.Realm) error {
	prefix := fmt.Sprintf("%s/%s/", realm.Region.Name, realm.Slug)
	it := pricelistHistoriesBucket.Objects(storeClient.Context, &storage.Query{Prefix: prefix})
	for {
		objAttrs, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}

			if err != nil {
				return err
			}
		}

		if objAttrs.Metadata == nil {
			logging.Info("Add missing version-id")

			obj := pricelistHistoriesBucket.Object(objAttrs.Name)
			nextAttrs := storage.ObjectAttrsToUpdate{
				Metadata: map[string]string{"version_id": uuid.NewV4().String()},
			}
			if _, err := obj.Update(storeClient.Context, nextAttrs); err != nil {
				return err
			}
		}
	}

	return nil
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func BullshitIntake(_ context.Context, m PubSubMessage) error {
	obj, err := bootBase.GetFirmObject("bullshit.txt", bootBucket)
	if err != nil {
		return err
	}
	reader, err := obj.NewReader(storeClient.Context)
	if err != nil {
		return err
	}
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	shit := "SetMissingVersion\n"
	if string(data) != shit {
		logging.Info("Unmatched")

		return nil
	}

	job, err := func() (bus.CollectAuctionsJob, error) {
		var in bus.Message
		if err := json.Unmarshal(m.Data, &in); err != nil {
			return bus.CollectAuctionsJob{}, err
		}

		var out bus.CollectAuctionsJob
		if err := json.Unmarshal([]byte(in.Data), &out); err != nil {
			return bus.CollectAuctionsJob{}, err
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

	logging.WithField("realm", realm).Info("Derp")

	return SetMissingVersion(realm)
}
