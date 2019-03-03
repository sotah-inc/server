package store

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/util"
	"google.golang.org/api/iterator"
)

func NewAuctionManifestBase(c Client) AuctionManifestBase {
	return AuctionManifestBase{base{client: c}}
}

type AuctionManifestBase struct {
	base
}

func (b AuctionManifestBase) getBucketName(realm sotah.Realm) string {
	return fmt.Sprintf("auctions-manifest_%s_%s", realm.Region.Name, realm.Slug)
}

func (b AuctionManifestBase) GetBucket(realm sotah.Realm) *storage.BucketHandle {
	return b.base.getBucket(b.getBucketName(realm))
}

func (b AuctionManifestBase) ResolveBucket(realm sotah.Realm) (*storage.BucketHandle, error) {
	return b.base.resolveBucket(b.getBucketName(realm))
}

func (b AuctionManifestBase) getObjectName(targetTimestamp sotah.UnixTimestamp) string {
	return fmt.Sprintf("%d.json", targetTimestamp)
}

func (b AuctionManifestBase) GetObject(targetTimestamp sotah.UnixTimestamp, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return b.base.getObject(b.getObjectName(targetTimestamp), bkt)
}

func (b AuctionManifestBase) Handle(targetTimestamp sotah.UnixTimestamp, realm sotah.Realm) error {
	bkt, err := b.ResolveBucket(realm)
	if err != nil {
		return err
	}

	normalizedTargetTimestamp := sotah.UnixTimestamp(sotah.NormalizeTargetDate(time.Unix(int64(targetTimestamp), 0)).Unix())

	obj := b.GetObject(normalizedTargetTimestamp, bkt)
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

type DeleteJob struct {
	Err   error
	Realm sotah.Realm
	Count int
}

func (b AuctionManifestBase) DeleteAll(regionRealms map[blizzard.RegionName]sotah.Realms) chan DeleteJob {
	// spinning up the workers
	in := make(chan sotah.Realm)
	out := make(chan DeleteJob)
	worker := func() {
		for realm := range in {
			bkt, err := b.ResolveBucket(realm)
			if err != nil {
				out <- DeleteJob{
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
						out <- DeleteJob{
							Err:   nil,
							Realm: realm,
							Count: count,
						}

						break
					}

					if err != nil {
						out <- DeleteJob{
							Err:   err,
							Realm: realm,
							Count: count,
						}

						break
					}
				}

				obj := bkt.Object(objAttrs.Name)
				if err := obj.Delete(b.client.Context); err != nil {
					out <- DeleteJob{
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

func (b AuctionManifestBase) WriteAll(bkt *storage.BucketHandle, manifests map[sotah.UnixTimestamp]sotah.AuctionManifest) chan WriteAllOutJob {
	// spinning up the workers
	in := make(chan WriteAllInJob)
	out := make(chan WriteAllOutJob)
	worker := func() {
		for inJob := range in {
			obj := b.GetObject(inJob.NormalizedTimestamp, bkt)

			jsonEncodedBody, err := json.Marshal(inJob.Manifest)
			if err != nil {
				out <- WriteAllOutJob{
					Err:                 err,
					NormalizedTimestamp: inJob.NormalizedTimestamp,
				}

				continue
			}

			gzipEncodedBody, err := util.GzipEncode(jsonEncodedBody)
			if err != nil {
				out <- WriteAllOutJob{
					Err:                 err,
					NormalizedTimestamp: inJob.NormalizedTimestamp,
				}

				continue
			}

			wc := obj.NewWriter(b.client.Context)
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
