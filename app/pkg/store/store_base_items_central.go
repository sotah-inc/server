package store

import (
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
)

func NewItemsCentralBase(c Client, location string) ItemsCentralBase {
	return ItemsCentralBase{base{client: c, location: location}}
}

type ItemsCentralBase struct {
	base
}

func (b ItemsCentralBase) getBucketName() string {
	return "sotah-items-us-central1"
}

func (b ItemsCentralBase) GetBucket() *storage.BucketHandle {
	return b.base.getBucket(b.getBucketName())
}

func (b ItemsCentralBase) GetFirmBucket() (*storage.BucketHandle, error) {
	return b.base.getFirmBucket(b.getBucketName())
}

func (b ItemsCentralBase) resolveBucket() (*storage.BucketHandle, error) {
	return b.base.resolveBucket(b.getBucketName())
}

func (b ItemsCentralBase) getObjectName(id blizzard.ItemID) string {
	return fmt.Sprintf("%d.json.gz", id)
}

func (b ItemsCentralBase) GetObject(id blizzard.ItemID, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return b.base.getObject(b.getObjectName(id), bkt)
}

func (b ItemsCentralBase) GetFirmObject(id blizzard.ItemID, bkt *storage.BucketHandle) (*storage.ObjectHandle, error) {
	return b.base.getFirmObject(b.getObjectName(id), bkt)
}

func (b ItemsCentralBase) ObjectExists(id blizzard.ItemID, bkt *storage.BucketHandle) (bool, error) {
	return b.base.ObjectExists(b.GetObject(id, bkt))
}
