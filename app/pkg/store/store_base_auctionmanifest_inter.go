package store

import (
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/sotah"
)

func NewAuctionManifestBaseInter(c Client, location string) AuctionManifestBaseInter {
	return AuctionManifestBaseInter{base{client: c, location: location}}
}

type AuctionManifestBaseInter struct {
	base
}

func (b AuctionManifestBaseInter) getBucketName() string {
	return "auctions-manifest-us-central1"
}

func (b AuctionManifestBaseInter) GetBucket() *storage.BucketHandle {
	return b.base.getBucket(b.getBucketName())
}

func (b AuctionManifestBaseInter) ResolveBucket() (*storage.BucketHandle, error) {
	return b.base.resolveBucket(b.getBucketName())
}

func (b AuctionManifestBaseInter) GetFirmBucket() (*storage.BucketHandle, error) {
	return b.base.getFirmBucket(b.getBucketName())
}

func (b AuctionManifestBaseInter) GetObjectName(targetTimestamp sotah.UnixTimestamp, realm sotah.Realm) string {
	return fmt.Sprintf("%s/%s/%d.json", realm.Region.Name, realm.Slug, targetTimestamp)
}

func (b AuctionManifestBaseInter) GetObject(targetTimestamp sotah.UnixTimestamp, realm sotah.Realm, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return b.base.getObject(b.GetObjectName(targetTimestamp, realm), bkt)
}
