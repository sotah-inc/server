package store

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/sotah-inc/server/app/pkg/util"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/sotah"
)

func NewAuctionManifestBase(c Client) AuctionManifestBase {
	return AuctionManifestBase{base{client: c}}
}

type AuctionManifestBase struct {
	base
}

func (b AuctionManifestBase) getBucketName(realm sotah.Realm) string {
	return fmt.Sprintf("auctions-manifest_%s_%s", realm.Region.Name, realm.Slug)
}

func (b AuctionManifestBase) GetBucket(realm sotah.Realm) *storage.BucketHandle {
	return b.base.getBucket(b.getBucketName(realm))
}

func (b AuctionManifestBase) ResolveBucket(realm sotah.Realm) (*storage.BucketHandle, error) {
	return b.base.resolveBucket(b.getBucketName(realm))
}

func (b AuctionManifestBase) getObjectName(targetTimestamp sotah.UnixTimestamp) string {
	return fmt.Sprintf("%d.json", targetTimestamp)
}

func (b AuctionManifestBase) GetObject(targetTimestamp sotah.UnixTimestamp, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return b.base.getObject(b.getObjectName(targetTimestamp), bkt)
}

func (b AuctionManifestBase) Handle(targetTimestamp sotah.UnixTimestamp, realm sotah.Realm) error {
	bkt, err := b.ResolveBucket(realm)
	if err != nil {
		return err
	}

	normalizedTargetTimestamp := sotah.UnixTimestamp(sotah.NormalizeTargetDate(time.Unix(int64(targetTimestamp), 0)).Unix())

	obj := b.GetObject(normalizedTargetTimestamp, bkt)
	nextManifest, err := func() (sotah.AuctionManifest, error) {
		exists, err := b.ObjectExists(obj)
		if err != nil {
			return sotah.AuctionManifest{}, err
		}

		if !exists {
			return sotah.AuctionManifest{}, nil
		}

		reader, err := obj.NewReader(b.client.Context)
		if err != nil {
			return sotah.AuctionManifest{}, nil
		}

		data, err := ioutil.ReadAll(reader)
		if err != nil {
			return sotah.AuctionManifest{}, nil
		}

		var out sotah.AuctionManifest
		if err := json.Unmarshal(data, &out); err != nil {
			return sotah.AuctionManifest{}, nil
		}

		return out, nil
	}()
	if err != nil {
		return err
	}

	nextManifest = append(nextManifest, targetTimestamp)
	jsonEncodedBody, err := json.Marshal(nextManifest)
	if err != nil {
		return err
	}

	gzipEncodedBody, err := util.GzipEncode(jsonEncodedBody)
	if err != nil {
		return err
	}

	wc := obj.NewWriter(b.client.Context)
	wc.ContentType = "application/json"
	wc.ContentEncoding = "gzip"
	if _, err := wc.Write(gzipEncodedBody); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}

	return nil
}
