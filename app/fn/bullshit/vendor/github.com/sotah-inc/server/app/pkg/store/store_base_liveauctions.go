package store

import (
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
)

func NewLiveAuctionsBase(c Client) LiveAuctionsBase {
	return LiveAuctionsBase{base{client: c}}
}

type LiveAuctionsBase struct {
	base
}

func (b LiveAuctionsBase) getBucketName() string {
	return "live-auctions"
}

func (b LiveAuctionsBase) GetBucket() *storage.BucketHandle {
	return b.base.getBucket(b.getBucketName())
}

func (b LiveAuctionsBase) resolveBucket() (*storage.BucketHandle, error) {
	return b.base.resolveBucket(b.getBucketName())
}

func (b LiveAuctionsBase) getObjectName(realm sotah.Realm) string {
	return fmt.Sprintf("%s-%s.json.gz", realm.Region.Name, realm.Slug)
}

func (b LiveAuctionsBase) GetObject(realm sotah.Realm, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return b.base.getObject(b.getObjectName(realm), bkt)
}

func (b LiveAuctionsBase) Handle(aucs blizzard.Auctions, realm sotah.Realm) error {
	logging.WithField("bucket", b.getBucketName()).Info("Resolving bucket")

	bkt, err := b.resolveBucket()
	if err != nil {
		return err
	}

	// encoding auctions in the appropriate format
	gzipEncodedBody, err := sotah.NewMiniAuctionListFromMiniAuctions(sotah.NewMiniAuctions(aucs)).EncodeForDatabase()
	if err != nil {
		return err
	}

	// writing it out to the gcloud object
	wc := b.GetObject(realm, bkt).NewWriter(b.client.Context)
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
