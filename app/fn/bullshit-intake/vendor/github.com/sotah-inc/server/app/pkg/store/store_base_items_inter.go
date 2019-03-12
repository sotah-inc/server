package store

import (
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
)

func NewItemsBaseInter(c Client, location string) ItemsBaseInter {
	return ItemsBaseInter{base{client: c, location: location}}
}

type ItemsBaseInter struct {
	base
}

func (b ItemsBaseInter) getBucketName() string {
	return "sotah-items-us-central1"
}

func (b ItemsBaseInter) GetBucket() *storage.BucketHandle {
	return b.base.getBucket(b.getBucketName())
}

func (b ItemsBaseInter) resolveBucket() (*storage.BucketHandle, error) {
	return b.base.resolveBucket(b.getBucketName())
}

func (b ItemsBaseInter) getObjectName(id blizzard.ItemID) string {
	return fmt.Sprintf("%d.json.gz", id)
}

func (b ItemsBaseInter) GetObject(id blizzard.ItemID, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return b.base.getObject(b.getObjectName(id), bkt)
}
