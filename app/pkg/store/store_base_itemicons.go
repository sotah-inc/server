package store

import (
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/store/regions"
)

const StoreItemIconURLFormat = "https://storage.googleapis.com/%s/%s"

func NewItemIconsBase(c Client, location regions.Region) ItemIconsBase {
	return ItemIconsBase{base{client: c, location: location}}
}

type ItemIconsBase struct {
	base
}

func (b ItemIconsBase) GetBucketName() string {
	return "item-icons"
}

func (b ItemIconsBase) GetBucket() *storage.BucketHandle {
	return b.base.getBucket(b.GetBucketName())
}

func (b ItemIconsBase) GetFirmBucket() (*storage.BucketHandle, error) {
	return b.base.getFirmBucket(b.GetBucketName())
}

func (b ItemIconsBase) resolveBucket() (*storage.BucketHandle, error) {
	return b.base.resolveBucket(b.GetBucketName())
}

func (b ItemIconsBase) GetObjectName(name string) string {
	return fmt.Sprintf("%s.jpg", name)
}

func (b ItemIconsBase) GetObject(name string, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return b.base.getObject(b.GetObjectName(name), bkt)
}

func (b ItemIconsBase) GetFirmObject(name string, bkt *storage.BucketHandle) (*storage.ObjectHandle, error) {
	return b.base.getFirmObject(b.GetObjectName(name), bkt)
}
