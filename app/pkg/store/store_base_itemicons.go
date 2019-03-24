package store

import (
	"fmt"

	"cloud.google.com/go/storage"
)

func NewItemIconsBase(c Client, location string) ItemIconsBase {
	return ItemIconsBase{base{client: c, location: location}}
}

type ItemIconsBase struct {
	base
}

func (b ItemIconsBase) getBucketName() string {
	return "item-icons"
}

func (b ItemIconsBase) GetBucket() *storage.BucketHandle {
	return b.base.getBucket(b.getBucketName())
}

func (b ItemIconsBase) GetFirmBucket() (*storage.BucketHandle, error) {
	return b.base.getFirmBucket(b.getBucketName())
}

func (b ItemIconsBase) resolveBucket() (*storage.BucketHandle, error) {
	return b.base.resolveBucket(b.getBucketName())
}

func (b ItemIconsBase) getObjectName(name string) string {
	return fmt.Sprintf("%s.jpg", name)
}

func (b ItemIconsBase) GetObject(name string, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return b.base.getObject(b.getObjectName(name), bkt)
}

func (b ItemIconsBase) GetFirmObject(name string, bkt *storage.BucketHandle) (*storage.ObjectHandle, error) {
	return b.base.getFirmObject(b.getObjectName(name), bkt)
}
