package cleanupintake

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/store"
	"google.golang.org/api/iterator"
)

var projectId = os.Getenv("GCP_PROJECT")

var storeClient store.Client
var auctionManifestStoreBaseV2 store.AuctionManifestBaseV2
var auctionsStoreBaseV2 store.AuctionsBaseV2

var newManifestBucket *storage.BucketHandle

func init() {
	var err error

	storeClient, err = store.NewClient(projectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return
	}
	auctionsStoreBaseV2 = store.NewAuctionsBaseV2(storeClient)
	auctionManifestStoreBaseV2 = store.NewAuctionManifestBaseV2(storeClient)

	newManifestBucket, err = auctionManifestStoreBaseV2.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get new manifest bucket: %s", err.Error())

		return
	}
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func RebuildManifest(realm sotah.Realm) error {
	currentNormalizedTime := sotah.NormalizeTargetDate(time.Now())

	manifests := map[sotah.UnixTimestamp]sotah.AuctionManifest{}

	bkt := auctionsStoreBaseV2.GetBucket()
	it := bkt.Objects(
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

	for writeAllOutJob := range auctionManifestStoreBaseV2.WriteAll(newManifestBucket, realm, manifests) {
		if writeAllOutJob.Err != nil {
			return writeAllOutJob.Err
		}
	}

	return nil
}

func BullshitIntake(_ context.Context, m PubSubMessage) error {
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

	return RebuildManifest(realm)
}
