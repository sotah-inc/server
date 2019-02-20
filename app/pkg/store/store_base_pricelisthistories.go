package store

import (
	"fmt"
	"io/ioutil"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/sotah"
)

type PricelistHistoriesBase struct {
	base
}

func (b PricelistHistoriesBase) getBucketName(rea sotah.Realm) string {
	return fmt.Sprintf("pricelist-histories_%s_%s", rea.Region.Name, rea.Slug)
}

func (b PricelistHistoriesBase) getBucket(rea sotah.Realm) *storage.BucketHandle {
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

func (b PricelistHistoriesBase) handle(aucs blizzard.Auctions, targetTime time.Time, rea sotah.Realm) error {
	// gathering the bucket
	bkt, err := b.resolveBucket(rea)
	if err != nil {
		return err
	}

	// gathering an object
	obj := b.getObject(targetTime, bkt)

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

		return sotah.NewItemPriceHistoriesFromGzipped(body)
	}()

	// resolving unix-timestamp of target-time
	targetTimestamp := sotah.UnixTimestamp(targetTime.Unix())

	// gathering new item-prices from the input
	iPrices := sotah.NewItemPrices(sotah.NewMiniAuctionListFromMiniAuctions(sotah.NewMiniAuctions(aucs)))

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

		ipHistories[itemId] = pHistory
	}

	// encoding the item-price-histories for persistence
	gzipEncodedBody, err := ipHistories.EncodeForPersistence()
	if err != nil {
		return err
	}

	// writing it out to the gcloud object
	wc := obj.NewWriter(b.client.Context)
	wc.ContentType = "application/json"
	wc.ContentEncoding = "gzip"
	if _, err := wc.Write(gzipEncodedBody); err != nil {
		return err
	}

	return wc.Close()
}
