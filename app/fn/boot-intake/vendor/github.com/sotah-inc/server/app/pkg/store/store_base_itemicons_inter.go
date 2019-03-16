package store

import (
	"fmt"

	"cloud.google.com/go/storage"
)

func NewItemIconsInter(c Client, location string) ItemIconsInter {
	return ItemIconsInter{base{client: c, location: location}}
}

type ItemIconsInter struct {
	base
}

func (b ItemIconsInter) getBucketName() string {
	return "item-icons-us-central1"
}

func (b ItemIconsInter) GetBucket() *storage.BucketHandle {
	return b.base.getBucket(b.getBucketName())
}

func (b ItemIconsInter) resolveBucket() (*storage.BucketHandle, error) {
	return b.base.resolveBucket(b.getBucketName())
}

func (b ItemIconsInter) getObjectName(name string) string {
	return fmt.Sprintf("%s.jpg", name)
}

func (b ItemIconsInter) GetObject(name string, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return b.base.getObject(b.getObjectName(name), bkt)
}
