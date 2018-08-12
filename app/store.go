package main

import (
	"context"
	"fmt"
	"time"

	storage "cloud.google.com/go/storage"
)

func newStore(projectID string) (store, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return store{}, err
	}

	return store{ctx, projectID, client}, nil
}

type store struct {
	context   context.Context
	projectID string
	client    *storage.Client
}

func (sto store) getRealmBucketName(rea realm) string {
	return fmt.Sprintf("raw-auctions_%s_%s", rea.region.Name, rea.Slug)
}

func (sto store) getRealmBucket(rea realm) *storage.BucketHandle {
	return sto.client.Bucket(sto.getRealmBucketName(rea))
}

func (sto store) createRealmBucket(rea realm) (*storage.BucketHandle, error) {
	bkt := sto.getRealmBucket(rea)
	err := bkt.Create(sto.context, sto.projectID, &storage.BucketAttrs{
		StorageClass: "REGIONAL",
		Location:     "us-east1",
	})
	if err != nil {
		return nil, err
	}

	return bkt, nil
}

func (sto store) realmBucketExists(rea realm) (bool, error) {
	_, err := sto.getRealmBucket(rea).Attrs(sto.context)
	if err != nil {
		if err != storage.ErrBucketNotExist {
			return false, err
		}

		return false, nil
	}

	return true, nil
}

func (sto store) resolveRealmBucket(rea realm) (*storage.BucketHandle, error) {
	exists, err := sto.realmBucketExists(rea)
	if err != nil {
		return nil, err
	}

	if !exists {
		return sto.createRealmBucket(rea)
	}

	return sto.getRealmBucket(rea), nil
}

func (sto store) getRealmAuctionsObjectName(lastModified time.Time) string {
	return fmt.Sprintf("%d.json.gz", lastModified.Unix())
}

func (sto store) writeRealmAuctions(rea realm, lastModified time.Time, body []byte) error {
	bkt, err := sto.resolveRealmBucket(rea)
	if err != nil {
		return err
	}

	wc := bkt.Object(sto.getRealmAuctionsObjectName(lastModified)).NewWriter(sto.context)
	wc.ContentType = "application/json"
	wc.ContentEncoding = "gzip"
	wc.Write(body)
	return wc.Close()
}
