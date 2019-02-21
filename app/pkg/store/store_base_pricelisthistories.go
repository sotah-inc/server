package store

import (
	"fmt"
	"io/ioutil"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
)

func NewPricelistHistoriesBase(c Client) PricelistHistoriesBase {
	return PricelistHistoriesBase{base{client: c}}
}

type PricelistHistoriesBase struct {
	base
}

func (b PricelistHistoriesBase) getBucketName(rea sotah.Realm) string {
	return fmt.Sprintf("pricelist-histories_%s_%s", rea.Region.Name, rea.Slug)
}

func (b PricelistHistoriesBase) GetBucket(rea sotah.Realm) *storage.BucketHandle {
	return b.base.getBucket(b.getBucketName(rea))
}

func (b PricelistHistoriesBase) resolveBucket(rea sotah.Realm) (*storage.BucketHandle, error) {
	return b.base.resolveBucket(b.getBucketName(rea))
}

func (b PricelistHistoriesBase) getObjectName(targetTime time.Time) string {
	return fmt.Sprintf("%d.json.gz", targetTime.Unix())
}

func (b PricelistHistoriesBase) getObject(targetTime time.Time, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return b.base.getObject(b.getObjectName(targetTime), bkt)
}

func (b PricelistHistoriesBase) Handle(aucs blizzard.Auctions, targetTime time.Time, rea sotah.Realm) error {
	normalizedTargetDate := sotah.NormalizeTargetDate(targetTime)

	entry := logging.WithFields(logrus.Fields{
		"region":                 rea.Region.Name,
		"realm":                  rea.Slug,
		"normalized-target-date": normalizedTargetDate.Unix(),
	})
	entry.Info("Handling, resolving bucket")

	// gathering the bucket
	bkt, err := b.resolveBucket(rea)
	if err != nil {
		return err
	}

	// gathering an object
	obj := b.getObject(normalizedTargetDate, bkt)

	entry.Info("Resolved bucket, resolving item-price-histories")

	// resolving item-price-histories
	ipHistories, err := func() (sotah.ItemPriceHistories, error) {
		exists, err := b.objectExists(obj)
		if err != nil {
			return sotah.ItemPriceHistories{}, err
		}

		if !exists {
			return sotah.ItemPriceHistories{}, nil
		}

		reader, err := obj.NewReader(b.client.Context)
		if err != nil {
			return sotah.ItemPriceHistories{}, err
		}
		defer reader.Close()

		body, err := ioutil.ReadAll(reader)
		if err != nil {
			return sotah.ItemPriceHistories{}, err
		}

		entry.WithField("length", len(body)).Info("Decoding item-price-histories")

		return sotah.NewItemPriceHistoriesFromGzipped(body)
	}()

	// resolving unix-timestamp of target-time
	targetTimestamp := sotah.UnixTimestamp(targetTime.Unix())

	entry.WithField("item-price-histories", len(ipHistories)).Info("Resolved item-price-histories, resolving item-prices from auctions")

	// gathering new item-prices from the input
	iPrices := sotah.NewItemPrices(sotah.NewMiniAuctionListFromMiniAuctions(sotah.NewMiniAuctions(aucs)))

	entry.WithField("items", len(iPrices)).Info("Resolved item-prices from auctions, merging item-prices in")

	// merging item-prices into the item-price-histories
	for itemId, prices := range iPrices {
		pHistory := func() sotah.PriceHistory {
			result, ok := ipHistories[itemId]
			if !ok {
				return sotah.PriceHistory{}
			}

			return result
		}()
		pHistory[targetTimestamp] = prices

		if len(pHistory) > 1 {
			entry.WithField("item-id", itemId).Info("Found with longer than 1 entry")
		}

		ipHistories[itemId] = pHistory
	}

	entry.WithField("target-timestamp", targetTimestamp).Info("Merged item-prices in, encoding item-price-histories for persistence")

	// encoding the item-price-histories for persistence
	gzipEncodedBody, err := ipHistories.EncodeForPersistence()
	if err != nil {
		return err
	}

	entry.Info("Encoded item-price-histories, writing to gcloud obj")

	// writing it out to the gcloud object
	wc := obj.NewWriter(b.client.Context)
	wc.ContentType = "application/json"
	wc.ContentEncoding = "gzip"
	if _, err := wc.Write(gzipEncodedBody); err != nil {
		return err
	}

	entry.Info("Written to gcloud obj, closing")

	return wc.Close()
}
