package main

import (
	"context"
	"fmt"
	"time"

	storage "cloud.google.com/go/storage"
	"github.com/ihsw/sotah-server/app/util"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
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

	log.WithFields(log.Fields{
		"region": rea.region.Name,
		"realm":  rea.Slug,
		"length": len(body),
	}).Debug("Writing auctions to gcloud storage")

	wc := bkt.Object(sto.getRealmAuctionsObjectName(lastModified)).NewWriter(sto.context)
	wc.ContentType = "application/json"
	wc.ContentEncoding = "gzip"
	wc.Write(body)
	return wc.Close()
}

func (sto store) getTotalRealmAuctionsSize(rea realm) (int64, error) {
	log.WithFields(log.Fields{
		"region": rea.region.Name,
		"realm":  rea.Slug,
	}).Debug("Gathering total bucket size")

	exists, err := sto.realmBucketExists(rea)
	if err != nil {
		return 0, err
	}

	if !exists {
		return 0, nil
	}

	it := sto.getRealmBucket(rea).Objects(sto.context, nil)
	totalSize := int64(0)
	for {
		objAttrs, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}

			return 0, err
		}

		totalSize += objAttrs.Size
	}

	return totalSize, nil
}

type getTotalRealmAuctionSizeJob struct {
	realm     realm
	totalSize int64
	err       error
}

func (sto store) getTotalRealmsAuctionSize(reas realms) chan getTotalRealmAuctionSizeJob {
	// establishing channels
	out := make(chan getTotalRealmAuctionSizeJob)
	in := make(chan realm)

	// spinning up the workers for gathering total realm auction size
	worker := func() {
		for rea := range in {
			totalSize, err := sto.getTotalRealmAuctionsSize(rea)
			out <- getTotalRealmAuctionSizeJob{rea, totalSize, err}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	// queueing up the realms
	go func() {
		for _, rea := range reas {
			in <- rea
		}

		close(in)
	}()

	return out
}
