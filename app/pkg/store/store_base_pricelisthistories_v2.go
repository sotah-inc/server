package store

import (
	"fmt"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/sotah"
)

func NewPricelistHistoriesBaseV2(c Client) PricelistHistoriesBaseV2 {
	return PricelistHistoriesBaseV2{base{client: c}}
}

type PricelistHistoriesBaseV2 struct {
	base
}

func (b PricelistHistoriesBaseV2) getBucketName() string {
	return "pricelist-histories"
}

func (b PricelistHistoriesBaseV2) GetBucket() *storage.BucketHandle {
	return b.base.getBucket(b.getBucketName())
}

func (b PricelistHistoriesBaseV2) GetFirmBucket() (*storage.BucketHandle, error) {
	return b.base.getFirmBucket(b.getBucketName())
}

func (b PricelistHistoriesBaseV2) getObjectName(targetTime time.Time, realm sotah.Realm) string {
	return fmt.Sprintf("%s/%s/%d.txt.gz", realm.Region.Name, realm.Slug, targetTime.Unix())
}

func (b PricelistHistoriesBaseV2) GetObject(targetTime time.Time, realm sotah.Realm, bkt *storage.BucketHandle) *storage.ObjectHandle {
	return b.base.getObject(b.getObjectName(targetTime, realm), bkt)
}

func (b PricelistHistoriesBaseV2) Handle(aucs blizzard.Auctions, targetTime time.Time, rea sotah.Realm) (sotah.UnixTimestamp, error) {
	normalizedTargetDate := sotah.NormalizeTargetDate(targetTime)

	// resolving unix-timestamp of target-time
	targetTimestamp := sotah.UnixTimestamp(targetTime.Unix())

	// gathering the bucket
	bkt, err := b.GetFirmBucket()
	if err != nil {
		return 0, err
	}

	// gathering an object
	obj := b.GetObject(normalizedTargetDate, rea, bkt)

	// resolving item-price-histories
	ipHistories, err := func() (sotah.ItemPriceHistories, error) {
		exists, err := b.ObjectExists(obj)
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

		return sotah.NewItemPriceHistoriesFromMinimized(reader)
	}()
	if err != nil {
		return 0, err
	}

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
		return 0, err
	}

	// writing it out to the gcloud object
	wc := obj.NewWriter(b.client.Context)
	wc.ContentType = "text/plain"
	wc.ContentEncoding = "gzip"
	if _, err := wc.Write(gzipEncodedBody); err != nil {
		return 0, err
	}
	if err := wc.Close(); err != nil {
		return 0, err
	}

	return sotah.UnixTimestamp(normalizedTargetDate.Unix()), wc.Close()
}
