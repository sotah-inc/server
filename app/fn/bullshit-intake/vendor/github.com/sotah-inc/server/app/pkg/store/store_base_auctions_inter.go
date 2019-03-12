package store

import (
	"fmt"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/sotah"
)

func NewAuctionsBaseInter(c Client, location string) AuctionsBaseInter {
	return AuctionsBaseInter{base{client: c, location: location}}
}

type AuctionsBaseInter struct {
	base
}

func (b AuctionsBaseInter) getBucketName() string {
	return "raw-auctions"
}

func (b AuctionsBaseInter) GetBucket() *storage.BucketHandle {
	return b.base.getBucket(b.getBucketName())
}

func (b AuctionsBaseInter) GetFirmBucket() (*storage.BucketHandle, error) {
	return b.base.getFirmBucket(b.getBucketName())
}

func (b AuctionsBaseInter) ResolveBucket() (*storage.BucketHandle, error) {
	return b.base.resolveBucket(b.getBucketName())
}

func (b AuctionsBaseInter) getObjectName(realm sotah.Realm, lastModified time.Time) string {
	return fmt.Sprintf("%s/%s/%d.json.gz", realm.Region.Name, realm.Slug, lastModified.Unix())
}

func (b AuctionsBaseInter) GetObject(realm sotah.Realm, lastModified time.Time, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return b.base.getObject(b.getObjectName(realm, lastModified), bkt)
}

func (b AuctionsBaseInter) GetFirmObject(realm sotah.Realm, lastModified time.Time, bkt *storage.BucketHandle) (*storage.ObjectHandle, error) {
	return b.base.getFirmObject(b.getObjectName(realm, lastModified), bkt)
}
