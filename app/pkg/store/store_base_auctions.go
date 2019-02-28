package store

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/sotah-inc/server/app/pkg/util"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
)

func NewAuctionsBase(c Client) AuctionsBase {
	return AuctionsBase{base{client: c}}
}

type AuctionsBase struct {
	base
}

func (b AuctionsBase) getBucketName(realm sotah.Realm) string {
	return fmt.Sprintf("raw-auctions_%s_%s", realm.Region.Name, realm.Slug)
}

func (b AuctionsBase) GetBucket(realm sotah.Realm) *storage.BucketHandle {
	return b.base.getBucket(b.getBucketName(realm))
}

func (b AuctionsBase) resolveBucket(realm sotah.Realm) (*storage.BucketHandle, error) {
	return b.base.resolveBucket(b.getBucketName(realm))
}

func (b AuctionsBase) getObjectName(lastModified time.Time) string {
	return fmt.Sprintf("%d.json.gz", lastModified.Unix())
}

func (b AuctionsBase) GetObject(lastModified time.Time, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return b.base.getObject(b.getObjectName(lastModified), bkt)
}

func (b AuctionsBase) Handle(aucs blizzard.Auctions, lastModified time.Time, realm sotah.Realm) error {
	logging.WithField("bucket", b.getBucketName(realm)).Info("Resolving bucket")

	bkt, err := b.resolveBucket(realm)
	if err != nil {
		return err
	}

	logging.WithFields(logrus.Fields{
		"bucket": b.getBucketName(realm),
		"object": b.getObjectName(lastModified),
	}).Info("Resolved bucket, gathering object and encoding auctions")

	obj := b.GetObject(lastModified, bkt)

	jsonEncodedBody, err := json.Marshal(aucs)
	if err != nil {
		return err
	}

	gzipEncodedBody, err := util.GzipEncode(jsonEncodedBody)
	if err != nil {
		return err
	}

	logging.WithFields(logrus.Fields{
		"bucket":   b.getBucketName(realm),
		"object":   b.getObjectName(lastModified),
		"auctions": len(gzipEncodedBody),
	}).Info("Encoded auctions, writing to storage")

	// writing it out to the gcloud object
	wc := obj.NewWriter(b.client.Context)
	wc.ContentType = "application/json"
	wc.ContentEncoding = "gzip"
	if _, err := wc.Write(gzipEncodedBody); err != nil {
		return err
	}

	logging.WithFields(logrus.Fields{
		"bucket":   b.getBucketName(realm),
		"object":   b.getObjectName(lastModified),
		"auctions": len(gzipEncodedBody),
	}).Info("Written to storage")

	return nil
}
