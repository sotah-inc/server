package store

import (
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/sotah"
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
