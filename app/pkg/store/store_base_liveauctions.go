package store

import (
	"errors"
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
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
	return errors.New("wew lad")
}
