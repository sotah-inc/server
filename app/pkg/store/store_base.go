package store

import (
	"errors"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/sotah"
)

type base struct {
	client       Client
	storageClass string
	location     string
}

func (b base) getBucket(name string) *storage.BucketHandle {
	return b.client.client.Bucket(name)
}

func (b base) createBucket(bkt *storage.BucketHandle) error {
	return bkt.Create(b.client.Context, b.client.projectID, &storage.BucketAttrs{
		StorageClass: b.storageClass,
		Location:     b.location,
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

func (b base) getFirmObject(name string, bkt *storage.BucketHandle) (*storage.ObjectHandle, error) {
	obj := bkt.Object(name)
	exists, err := b.ObjectExists(obj)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.New("obj does not exist")
	}

	return obj, nil
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

type RealmTimestamps map[blizzard.RealmSlug][]sotah.UnixTimestamp

type RegionRealmTimestamps map[blizzard.RegionName]RealmTimestamps

type GetTimestampsJob struct {
	Err        error
	Realm      sotah.Realm
	Timestamps []sotah.UnixTimestamp
}

func (b base) Write(wc *storage.Writer, body []byte) error {
	if _, err := wc.Write(body); err != nil {
		return err
	}

	return wc.Close()
}
