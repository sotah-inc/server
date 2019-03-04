package store

import (
	"errors"

	"cloud.google.com/go/storage"
)

type base struct {
	client Client
}

func (b base) getBucket(name string) *storage.BucketHandle {
	return b.client.client.Bucket(name)
}

func (b base) createBucket(bkt *storage.BucketHandle) error {
	return bkt.Create(b.client.Context, b.client.projectID, &storage.BucketAttrs{
		StorageClass: "REGIONAL",
		Location:     "us-east1",
	})
}

func (b base) BucketExists(bkt *storage.BucketHandle) (bool, error) {
	_, err := bkt.Attrs(b.client.Context)
	if err != nil {
		if err != storage.ErrBucketNotExist {
			return false, err
		}

		return false, nil
	}

	return true, nil
}

func (b base) resolveBucket(name string) (*storage.BucketHandle, error) {
	bkt := b.getBucket(name)

	exists, err := b.BucketExists(bkt)
	if err != nil {
		return nil, err
	}

	if !exists {
		if err := b.createBucket(bkt); err != nil {
			return nil, err
		}

		return bkt, nil
	}

	return bkt, nil
}

func (b base) getFirmBucket(name string) (*storage.BucketHandle, error) {
	bkt := b.getBucket(name)
	exists, err := b.BucketExists(bkt)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.New("bucket does not exist")
	}

	return bkt, nil
}

func (b base) getObject(name string, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return bkt.Object(name)
}

func (b base) ObjectExists(obj *storage.ObjectHandle) (bool, error) {
	_, err := obj.Attrs(b.client.Context)
	if err != nil {
		if err != storage.ErrObjectNotExist {
			return false, err
		}

		return false, nil
	}

	return true, nil
}
