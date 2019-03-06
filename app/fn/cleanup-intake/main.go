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
	"github.com/sotah-inc/server/app/pkg/util"
	"google.golang.org/api/iterator"
)

var projectId = os.Getenv("GCP_PROJECT")

var storeClient store.Client
var auctionManifestStoreBase store.AuctionManifestBase
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
	auctionManifestStoreBase = store.NewAuctionManifestBase(storeClient)
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

type TransferManifestsOutJob struct {
	Err             error
	ManifestObjName string
	Created         bool
	Updated         bool
}

func TransferManifests(realm sotah.Realm) error {
	manifestBucket, err := auctionManifestStoreBase.GetFirmBucket(realm)
	if err != nil {
		return err
	}

	oldManifestBucket := auctionManifestStoreBase.GetBucket(realm)
	exists, err := auctionManifestStoreBase.BucketExists(oldManifestBucket)
	if err != nil {
		return err
	}
	if !exists {
		logging.WithFields(logrus.Fields{
			"region": realm.Region.Name,
			"realm":  realm.Slug,
		}).Info("Old manifest bucket does not exist!")

		return nil
	}

	// spinning up the workers
	in := make(chan *storage.ObjectAttrs)
	out := make(chan TransferManifestsOutJob)
	worker := func() {
		for objAttrs := range in {
			parts := strings.Split(objAttrs.Name, ".")
			if _, err := strconv.Atoi(parts[0]); err != nil {
				out <- TransferManifestsOutJob{
					Err:             err,
					ManifestObjName: objAttrs.Name,
				}

				continue
			}

			objTimestamp, err := strconv.Atoi(parts[0])
			if err != nil {
				out <- TransferManifestsOutJob{
					Err:             err,
					ManifestObjName: objAttrs.Name,
				}

				continue
			}

			newObj := auctionManifestStoreBaseV2.GetObject(sotah.UnixTimestamp(objTimestamp), realm, newManifestBucket)
			exists, err := auctionManifestStoreBaseV2.ObjectExists(newObj)
			if err != nil {
				out <- TransferManifestsOutJob{
					Err:             err,
					ManifestObjName: objAttrs.Name,
				}

				continue
			}

			if !exists {
				logging.WithFields(logrus.Fields{
					"region": realm.Region.Name,
					"realm":  realm.Slug,
					"obj":    objAttrs.Name,
				}).Info("No obj exists, creating")

				oldObj := auctionManifestStoreBase.GetObject(sotah.UnixTimestamp(objTimestamp), oldManifestBucket)
				if _, err := newObj.CopierFrom(oldObj).Run(storeClient.Context); err != nil {
					out <- TransferManifestsOutJob{
						Err:             err,
						ManifestObjName: objAttrs.Name,
					}

					continue
				}

				out <- TransferManifestsOutJob{
					Err:             nil,
					ManifestObjName: objAttrs.Name,
					Created:         true,
				}

				continue
			}

			newManifest, err := auctionManifestStoreBaseV2.NewAuctionManifest(newObj)
			if err != nil {
				out <- TransferManifestsOutJob{
					Err:             err,
					ManifestObjName: objAttrs.Name,
				}

				continue
			}

			oldManifest, err := auctionManifestStoreBaseV2.NewAuctionManifest(auctionManifestStoreBase.GetObject(sotah.UnixTimestamp(objTimestamp), oldManifestBucket))
			if err != nil {
				out <- TransferManifestsOutJob{
					Err:             err,
					ManifestObjName: objAttrs.Name,
				}

				continue
			}

			if newManifest.Includes(oldManifest) {
				logging.WithFields(logrus.Fields{
					"region": realm.Region.Name,
					"realm":  realm.Slug,
					"obj":    objAttrs.Name,
				}).Info("New obj includes old obj data, skipping")

				out <- TransferManifestsOutJob{
					Err:             nil,
					ManifestObjName: objAttrs.Name,
				}

				continue
			}

			logging.WithFields(logrus.Fields{
				"region": realm.Region.Name,
				"realm":  realm.Slug,
				"obj":    objAttrs.Name,
			}).Info("Merging old obj into new obj")

			gzipEncodedBody, err := newManifest.Merge(oldManifest).EncodeForPersistence()
			if err != nil {
				out <- TransferManifestsOutJob{
					Err:             nil,
					ManifestObjName: objAttrs.Name,
				}

				continue
			}

			wc := newObj.NewWriter(storeClient.Context)
			wc.ContentType = "application/json"
			wc.ContentEncoding = "gzip"
			if _, err := wc.Write(gzipEncodedBody); err != nil {
				out <- TransferManifestsOutJob{
					Err:             err,
					ManifestObjName: objAttrs.Name,
				}

				continue
			}
			if err := wc.Close(); err != nil {
				out <- TransferManifestsOutJob{
					Err:             err,
					ManifestObjName: objAttrs.Name,
				}

				continue
			}

			out <- TransferManifestsOutJob{
				Err:             nil,
				ManifestObjName: objAttrs.Name,
				Updated:         true,
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
					logging.WithField("error", err.Error()).Fatal("Failed to head to next iterator")
				}
			}

			in <- objAttrs
		}

		close(in)
	}()

	updateCount := 0
	transferCount := 0
	for outJob := range out {
		if outJob.Err != nil {
			return outJob.Err
		}

		if outJob.Created {
			transferCount += 1

			continue
		}

		if outJob.Updated {
			updateCount += 1

			continue
		}
	}

	if transferCount > 0 {
		logging.WithFields(logrus.Fields{
			"region":         realm.Region.Name,
			"realm":          realm.Slug,
			"transfer-count": transferCount,
		}).Info("Transferred")
	}
	if updateCount > 0 {
		logging.WithFields(logrus.Fields{
			"region":       realm.Region.Name,
			"realm":        realm.Slug,
			"update-count": updateCount,
		}).Info("Updated")
	}
	if transferCount == 0 && updateCount == 0 {
		logging.WithFields(logrus.Fields{
			"region": realm.Region.Name,
			"realm":  realm.Slug,
		}).Info("No more manifests to transfer or update, clearing bucket and deleting it")

		it := oldManifestBucket.Objects(storeClient.Context, nil)
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

			obj := oldManifestBucket.Object(objAttrs.Name)
			if err := obj.Delete(storeClient.Context); err != nil {
				return err
			}
		}

		if err := oldManifestBucket.Delete(storeClient.Context); err != nil {
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

	return TransferManifests(realm)
}
