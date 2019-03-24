package store

import (
	"fmt"

	"cloud.google.com/go/storage"
)

func NewItemIcons(c Client, location string) ItemIcons {
	return ItemIcons{base{client: c, location: location}}
}

type ItemIcons struct {
	base
}

func (b ItemIcons) getBucketName() string {
	return "item-icons"
}

func (b ItemIcons) GetBucket() *storage.BucketHandle {
	return b.base.getBucket(b.getBucketName())
}

func (b ItemIcons) GetFirmBucket() (*storage.BucketHandle, error) {
	return b.base.getFirmBucket(b.getBucketName())
}

func (b ItemIcons) resolveBucket() (*storage.BucketHandle, error) {
	return b.base.resolveBucket(b.getBucketName())
}

func (b ItemIcons) getObjectName(name string) string {
	return fmt.Sprintf("%s.jpg", name)
}

func (b ItemIcons) GetObject(name string, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return b.base.getObject(b.getObjectName(name), bkt)
}

func (b ItemIcons) GetFirmObject(name string, bkt *storage.BucketHandle) (*storage.ObjectHandle, error) {
	return b.base.getFirmObject(b.getObjectName(name), bkt)
}
