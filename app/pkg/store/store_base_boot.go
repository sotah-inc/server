package store

import (
	"cloud.google.com/go/storage"
)

func NewBootBase(c Client, location string) BootBase {
	return BootBase{base{client: c, location: location}}
}

type BootBase struct {
	base
}

func (b BootBase) getBucketName() string {
	return "boot"
}

func (b BootBase) GetBucket() *storage.BucketHandle {
	return b.base.getBucket(b.getBucketName())
}

func (b BootBase) GetFirmBucket() (*storage.BucketHandle, error) {
	return b.base.getFirmBucket(b.getBucketName())
}

func (b BootBase) GetObject(name string, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return b.base.getObject(name, bkt)
}

func (b BootBase) GetFirmObject(name string, bkt *storage.BucketHandle) (*storage.ObjectHandle, error) {
	return b.base.getFirmObject(name, bkt)
}
