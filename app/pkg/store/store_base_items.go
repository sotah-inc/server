package store

import (
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
)

func NewItemsBase(c Client, location string) ItemsBase {
	return ItemsBase{base{client: c, location: location}}
}

type ItemsBase struct {
	base
}

func (b ItemsBase) getBucketName() string {
	return "sotah-items"
}

func (b ItemsBase) GetBucket() *storage.BucketHandle {
	return b.base.getBucket(b.getBucketName())
}

func (b ItemsBase) GetFirmBucket() (*storage.BucketHandle, error) {
	return b.base.getFirmBucket(b.getBucketName())
}

func (b ItemsBase) resolveBucket() (*storage.BucketHandle, error) {
	return b.base.resolveBucket(b.getBucketName())
}

func (b ItemsBase) getObjectName(id blizzard.ItemID) string {
	return fmt.Sprintf("%d.json.gz", id)
}

func (b ItemsBase) GetObject(id blizzard.ItemID, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return b.base.getObject(b.getObjectName(id), bkt)
}

func (b ItemsBase) GetFirmObject(id blizzard.ItemID, bkt *storage.BucketHandle) (*storage.ObjectHandle, error) {
	return b.base.getFirmObject(b.getObjectName(id), bkt)
}
