package store

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/util"
	"google.golang.org/api/iterator"
)

func NewAuctionManifestBaseV2(c Client, location string) AuctionManifestBaseV2 {
	return AuctionManifestBaseV2{base{client: c, location: location}}
}

type AuctionManifestBaseV2 struct {
	base
}

func (b AuctionManifestBaseV2) getBucketName() string {
	return "auctions-manifest"
}

func (b AuctionManifestBaseV2) GetBucket() *storage.BucketHandle {
	return b.base.getBucket(b.getBucketName())
}

func (b AuctionManifestBaseV2) ResolveBucket() (*storage.BucketHandle, error) {
	return b.base.resolveBucket(b.getBucketName())
}

func (b AuctionManifestBaseV2) GetFirmBucket() (*storage.BucketHandle, error) {
	return b.base.getFirmBucket(b.getBucketName())
}

func (b AuctionManifestBaseV2) GetObjectName(targetTimestamp sotah.UnixTimestamp, realm sotah.Realm) string {
	return fmt.Sprintf("%s/%s/%d.json", realm.Region.Name, realm.Slug, targetTimestamp)
}

func (b AuctionManifestBaseV2) GetObject(targetTimestamp sotah.UnixTimestamp, realm sotah.Realm, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return b.base.getObject(b.GetObjectName(targetTimestamp, realm), bkt)
}

func (b AuctionManifestBaseV2) GetFirmObject(
	targetTimestamp sotah.UnixTimestamp,
	realm sotah.Realm,
	bkt *storage.BucketHandle,
) (*storage.ObjectHandle, error) {
	return b.base.getFirmObject(b.GetObjectName(targetTimestamp, realm), bkt)
}

func (b AuctionManifestBaseV2) Handle(targetTimestamp sotah.UnixTimestamp, realm sotah.Realm, bkt *storage.BucketHandle) error {
	normalizedTargetTimestamp := sotah.UnixTimestamp(sotah.NormalizeTargetDate(time.Unix(int64(targetTimestamp), 0)).Unix())

	obj := b.GetObject(normalizedTargetTimestamp, realm, bkt)
	nextManifest, err := func() (sotah.AuctionManifest, error) {
		exists, err := b.ObjectExists(obj)
		if err != nil {
			return sotah.AuctionManifest{}, err
		}

		if !exists {
			return sotah.AuctionManifest{}, nil
		}

		reader, err := obj.NewReader(b.client.Context)
		if err != nil {
			return sotah.AuctionManifest{}, nil
		}

		data, err := ioutil.ReadAll(reader)
		if err != nil {
			return sotah.AuctionManifest{}, nil
		}

		var out sotah.AuctionManifest
		if err := json.Unmarshal(data, &out); err != nil {
			return sotah.AuctionManifest{}, nil
		}

		return out, nil
	}()
	if err != nil {
		return err
	}

	nextManifest = append(nextManifest, targetTimestamp)
	jsonEncodedBody, err := json.Marshal(nextManifest)
	if err != nil {
		return err
	}

	gzipEncodedBody, err := util.GzipEncode(jsonEncodedBody)
	if err != nil {
		return err
	}

	wc := obj.NewWriter(b.client.Context)
	wc.ContentType = "application/json"
	wc.ContentEncoding = "gzip"
	if _, err := wc.Write(gzipEncodedBody); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}

	return nil
}

type DeleteAuctionManifestJob struct {
	Err   error
	Realm sotah.Realm
	Count int
}

func (b AuctionManifestBaseV2) DeleteAll(regionRealms map[blizzard.RegionName]sotah.Realms) chan DeleteAuctionManifestJob {
	// spinning up the workers
	in := make(chan sotah.Realm)
	out := make(chan DeleteAuctionManifestJob)
	worker := func() {
		for realm := range in {
			bkt, err := b.GetFirmBucket()
			if err != nil {
				out <- DeleteAuctionManifestJob{
					Err:   err,
					Realm: realm,
					Count: 0,
				}

				continue
			}

			it := bkt.Objects(b.client.Context, nil)
			count := 0
			for {
				objAttrs, err := it.Next()
				if err != nil {
					if err == iterator.Done {
						out <- DeleteAuctionManifestJob{
							Err:   nil,
							Realm: realm,
							Count: count,
						}

						break
					}

					if err != nil {
						out <- DeleteAuctionManifestJob{
							Err:   err,
							Realm: realm,
							Count: count,
						}

						break
					}
				}

				obj := bkt.Object(objAttrs.Name)
				if err := obj.Delete(b.client.Context); err != nil {
					out <- DeleteAuctionManifestJob{
						Err:   err,
						Realm: realm,
					}
					count++

					continue
				}
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(16, worker, postWork)

	// queueing it up
	go func() {
		for _, realms := range regionRealms {
			for _, realm := range realms {
				in <- realm
			}
		}

		close(in)
	}()

	return out
}

type WriteAllInJob struct {
	NormalizedTimestamp sotah.UnixTimestamp
	Manifest            sotah.AuctionManifest
}

type WriteAllOutJob struct {
	Err                 error
	NormalizedTimestamp sotah.UnixTimestamp
}

func (b AuctionManifestBaseV2) WriteAll(bkt *storage.BucketHandle, realm sotah.Realm, manifests map[sotah.UnixTimestamp]sotah.AuctionManifest) chan WriteAllOutJob {
	// spinning up the workers
	in := make(chan WriteAllInJob)
	out := make(chan WriteAllOutJob)
	worker := func() {
		for inJob := range in {
			gzipEncodedBody, err := inJob.Manifest.EncodeForPersistence()
			if err != nil {
				out <- WriteAllOutJob{
					Err:                 err,
					NormalizedTimestamp: inJob.NormalizedTimestamp,
				}

				continue
			}

			wc := b.GetObject(inJob.NormalizedTimestamp, realm, bkt).NewWriter(b.client.Context)
			wc.ContentType = "application/json"
			wc.ContentEncoding = "gzip"
			if _, err := wc.Write(gzipEncodedBody); err != nil {
				out <- WriteAllOutJob{
					Err:                 err,
					NormalizedTimestamp: inJob.NormalizedTimestamp,
				}

				continue
			}
			if err := wc.Close(); err != nil {
				out <- WriteAllOutJob{
					Err:                 err,
					NormalizedTimestamp: inJob.NormalizedTimestamp,
				}

				continue
			}

			out <- WriteAllOutJob{
				Err:                 nil,
				NormalizedTimestamp: inJob.NormalizedTimestamp,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(8, worker, postWork)

	// queueing it up
	go func() {
		for normalizedTimestamp, manifest := range manifests {
			in <- WriteAllInJob{
				NormalizedTimestamp: normalizedTimestamp,
				Manifest:            manifest,
			}
		}

		close(in)
	}()

	return out
}

func (b AuctionManifestBaseV2) NewAuctionManifest(obj *storage.ObjectHandle) (sotah.AuctionManifest, error) {
	reader, err := obj.NewReader(b.client.Context)
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
}

func (b AuctionManifestBaseV2) GetAllExpiredTimestamps(
	regionRealms map[blizzard.RegionName]sotah.Realms,
	bkt *storage.BucketHandle,
) (RegionRealmExpiredTimestamps, error) {
	out := make(chan GetExpiredTimestampsJob)
	in := make(chan sotah.Realm)

	// spinning up workers
	worker := func() {
		for realm := range in {
			timestamps, err := b.GetExpiredTimestamps(realm, bkt)
			if err != nil {
				out <- GetExpiredTimestampsJob{
					Err:   err,
					Realm: realm,
				}

				continue
			}

			out <- GetExpiredTimestampsJob{
				Err:        nil,
				Realm:      realm,
				Timestamps: timestamps,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(8, worker, postWork)

	// queueing it up
	go func() {
		for _, realms := range regionRealms {
			for _, realm := range realms {
				in <- realm
			}
		}

		close(in)
	}()

	// going over results
	expiredTimestamps := RegionRealmExpiredTimestamps{}
	for job := range out {
		if job.Err != nil {
			return RegionRealmExpiredTimestamps{}, job.Err
		}

		regionName := job.Realm.Region.Name
		if _, ok := expiredTimestamps[regionName]; !ok {
			expiredTimestamps[regionName] = RealmExpiredTimestamps{}
		}

		expiredTimestamps[regionName][job.Realm.Slug] = job.Timestamps
	}

	return expiredTimestamps, nil
}

func (b AuctionManifestBaseV2) GetExpiredTimestamps(realm sotah.Realm, bkt *storage.BucketHandle) ([]sotah.UnixTimestamp, error) {
	out := []sotah.UnixTimestamp{}

	limit := sotah.NormalizeTargetDate(time.Now()).AddDate(0, 0, -14)

	prefix := fmt.Sprintf("%s/%s/", realm.Region.Name, realm.Slug)
	it := bkt.Objects(b.client.Context, &storage.Query{Prefix: prefix})
	for {
		objAttrs, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}

			if err != nil {
				return []sotah.UnixTimestamp{}, err
			}
		}

		targetTimestamp, err := strconv.Atoi(objAttrs.Name[len(prefix):(len(objAttrs.Name) - len(".json"))])
		if err != nil {
			return []sotah.UnixTimestamp{}, err
		}

		targetTime := time.Unix(int64(targetTimestamp), 0)
		if targetTime.After(limit) {
			continue
		}

		out = append(out, sotah.UnixTimestamp(targetTimestamp))
	}

	return out, nil
}
