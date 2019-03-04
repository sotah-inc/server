package store

import (
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/util"
)

func NewAuctionsBaseV2(c Client) AuctionsBaseV2 {
	return AuctionsBaseV2{base{client: c}}
}

type AuctionsBaseV2 struct {
	base
}

func (b AuctionsBaseV2) getBucketName() string {
	return "raw-auctions"
}

func (b AuctionsBaseV2) GetBucket() *storage.BucketHandle {
	return b.base.getBucket(b.getBucketName())
}

func (b AuctionsBaseV2) GetFirmBucket() (*storage.BucketHandle, error) {
	return b.base.getFirmBucket(b.getBucketName())
}

func (b AuctionsBaseV2) ResolveBucket() (*storage.BucketHandle, error) {
	return b.base.resolveBucket(b.getBucketName())
}

func (b AuctionsBaseV2) getObjectName(realm sotah.Realm, lastModified time.Time) string {
	return fmt.Sprintf("%s/%s/%d.json.gz", realm.Region.Name, realm.Slug, lastModified.Unix())
}

func (b AuctionsBaseV2) GetObject(realm sotah.Realm, lastModified time.Time, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return b.base.getObject(b.getObjectName(realm, lastModified), bkt)
}

func (b AuctionsBaseV2) Handle(aucs blizzard.Auctions, lastModified time.Time, realm sotah.Realm, bkt *storage.BucketHandle) error {
	jsonEncodedBody, err := json.Marshal(aucs)
	if err != nil {
		return err
	}

	gzipEncodedBody, err := util.GzipEncode(jsonEncodedBody)
	if err != nil {
		return err
	}

	// writing it out to the gcloud object
	wc := b.GetObject(realm, lastModified, bkt).NewWriter(b.client.Context)
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

func (b AuctionsBaseV2) DeleteAll(bkt *storage.BucketHandle, realm sotah.Realm, manifest sotah.AuctionManifest) chan DeleteAuctionsJob {
	// spinning up the workers
	in := make(chan sotah.UnixTimestamp)
	out := make(chan DeleteAuctionsJob)
	worker := func() {
		for targetTimestamp := range in {
			obj := bkt.Object(b.getObjectName(realm, time.Unix(int64(targetTimestamp), 0)))
			if err := obj.Delete(b.client.Context); err != nil {
				out <- DeleteAuctionsJob{
					Err:             err,
					TargetTimestamp: targetTimestamp,
				}

				continue
			}

			out <- DeleteAuctionsJob{
				Err:             nil,
				TargetTimestamp: targetTimestamp,
			}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(16, worker, postWork)

	// queueing it up
	go func() {
		for _, targetTimestamp := range manifest {
			in <- targetTimestamp
		}

		close(in)
	}()

	return out
}
