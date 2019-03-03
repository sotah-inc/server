package store

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/sotah-inc/server/app/pkg/util"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/sotah"
)

func NewAuctionsBase(c Client) AuctionsBase {
	return AuctionsBase{base{client: c}}
}

type AuctionsBase struct {
	base
}

func (b AuctionsBase) getBucketName(realm sotah.Realm) string {
	return fmt.Sprintf("raw-auctions_%s_%s", realm.Region.Name, realm.Slug)
}

func (b AuctionsBase) GetBucket(realm sotah.Realm) *storage.BucketHandle {
	return b.base.getBucket(b.getBucketName(realm))
}

func (b AuctionsBase) ResolveBucket(realm sotah.Realm) (*storage.BucketHandle, error) {
	return b.base.resolveBucket(b.getBucketName(realm))
}

func (b AuctionsBase) getObjectName(lastModified time.Time) string {
	return fmt.Sprintf("%d.json.gz", lastModified.Unix())
}

func (b AuctionsBase) GetObject(lastModified time.Time, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return b.base.getObject(b.getObjectName(lastModified), bkt)
}

func (b AuctionsBase) Handle(aucs blizzard.Auctions, lastModified time.Time, bkt *storage.BucketHandle) error {
	jsonEncodedBody, err := json.Marshal(aucs)
	if err != nil {
		return err
	}

	gzipEncodedBody, err := util.GzipEncode(jsonEncodedBody)
	if err != nil {
		return err
	}

	// writing it out to the gcloud object
	wc := b.GetObject(lastModified, bkt).NewWriter(b.client.Context)
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

type DeleteAuctionsJob struct {
	Err             error
	TargetTimestamp sotah.UnixTimestamp
}

func (b AuctionsBase) DeleteAll(bkt *storage.BucketHandle, manifest sotah.AuctionManifest) chan DeleteAuctionsJob {
	// spinning up the workers
	in := make(chan sotah.UnixTimestamp)
	out := make(chan DeleteAuctionsJob)
	worker := func() {
		for targetTimestamp := range in {
			//obj := bkt.Object(b.getObjectName(time.Unix(int64(targetTimestamp), 0)))
			//if err := obj.Delete(b.client.Context); err != nil {
			//	out <- DeleteAuctionsJob{
			//		Err:             err,
			//		TargetTimestamp: targetTimestamp,
			//	}
			//
			//	continue
			//}

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
