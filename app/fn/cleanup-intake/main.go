package cleanupintake

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/util"

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
var auctionsStoreBase store.AuctionsBase
var auctionManifestStoreBase store.AuctionManifestBase
var auctionsStoreBaseV2 store.AuctionsBaseV2
var newAuctionsBucket *storage.BucketHandle

func init() {
	var err error

	storeClient, err = store.NewClient(projectId)
	if err != nil {
		log.Fatalf("Failed to create new store client: %s", err.Error())

		return
	}
	auctionsStoreBase = store.NewAuctionsBase(storeClient)
	auctionManifestStoreBase = store.NewAuctionManifestBase(storeClient)
	auctionsStoreBaseV2 = store.NewAuctionsBaseV2(storeClient)

	newAuctionsBucket, err = auctionsStoreBaseV2.GetFirmBucket()
	if err != nil {
		log.Fatalf("Failed to get new raw-auctions bucket: %s", err.Error())

		return
	}
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func RebuildManifest(realm sotah.Realm) error {
	currentNormalizedTime := sotah.NormalizeTargetDate(time.Now())

	manifests := map[sotah.UnixTimestamp]sotah.AuctionManifest{}

	bkt := auctionsStoreBase.GetBucket(realm)
	it := bkt.Objects(storeClient.Context, nil)
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

	manifestBucket, err := auctionManifestStoreBase.ResolveBucket(realm)
	if err != nil {
		return err
	}

	for writeAllOutJob := range auctionManifestStoreBase.WriteAll(manifestBucket, manifests) {
		if writeAllOutJob.Err != nil {
			return err
		}
	}

	return nil
}

func TransferRawAuctions(realm sotah.Realm) error {
	manifestBucket, err := auctionManifestStoreBase.GetFirmBucket(realm)
	if err != nil {
		return err
	}

	oldAuctionsBucket, err := auctionsStoreBase.GetFirmBucket(realm)
	if err != nil {
		return err
	}

	// spinning up the workers
	in := make(chan *storage.ObjectAttrs)
	out := make(chan error)
	worker := func() {
		for objAttrs := range in {
			manifest, err := func() (sotah.AuctionManifest, error) {
				obj := manifestBucket.Object(objAttrs.Name)
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
				out <- err

				continue
			}

			for _, targetTimestamp := range manifest {
				logging.WithFields(logrus.Fields{
					"region":           realm.Region.Name,
					"realm":            realm.Slug,
					"target-timestamp": targetTimestamp,
				}).Info("Transferring")

				targetTime := time.Unix(int64(targetTimestamp), 0)
				oldObj := auctionsStoreBase.GetObject(targetTime, oldAuctionsBucket)
				exists, err := auctionsStoreBase.ObjectExists(oldObj)
				if err != nil {
					out <- err

					continue
				}

				if !exists {
					continue
				}

				reader, err := oldObj.NewReader(storeClient.Context)
				if err != nil {
					out <- err

					continue
				}

				data, err := ioutil.ReadAll(reader)
				if err != nil {
					out <- err

					continue
				}

				gzipEncoded, err := util.GzipEncode(data)
				if err != nil {
					out <- err

					continue
				}

				newObj := auctionsStoreBaseV2.GetObject(realm, time.Unix(int64(targetTimestamp), 0), newAuctionsBucket)

				// writing it out to the gcloud object
				wc := newObj.NewWriter(storeClient.Context)
				wc.ContentType = "application/json"
				wc.ContentEncoding = "gzip"
				if _, err := wc.Write(gzipEncoded); err != nil {
					out <- err

					continue
				}
				if err := wc.Close(); err != nil {
					out <- err

					continue
				}

				break
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(8, worker, postWork)

	go func() {
		it := manifestBucket.Objects(storeClient.Context, nil)
		for {
			objAttrs, err := it.Next()
			if err != nil {
				if err == iterator.Done {
					break
				}

				if err != nil {
					break
				}
			}

			in <- objAttrs

			break
		}

		close(in)
	}()

	for err := range out {
		if err != nil {
			return err
		}
	}

	return nil
}

func CleanupIntake(_ context.Context, m PubSubMessage) error {
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

	return TransferRawAuctions(realm)
}
