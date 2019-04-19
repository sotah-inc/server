package store

import (
	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/store/regions"
)

func NewTransferBase(c Client, location regions.Region, bucketName string) TransferBase {
	return TransferBase{
		base:       base{client: c, location: location},
		bucketName: bucketName,
	}
}

type TransferBase struct {
	base

	bucketName string
}

func (b TransferBase) GetFirmBucket() (*storage.BucketHandle, error) {
	return b.base.getFirmBucket(b.bucketName)
}

func (b TransferBase) GetFirmObject(name string, bkt *storage.BucketHandle) (*storage.ObjectHandle, error) {
	return b.base.getFirmObject(name, bkt)
}

func (b TransferBase) GetObject(name string, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return b.base.getObject(name, bkt)
}
