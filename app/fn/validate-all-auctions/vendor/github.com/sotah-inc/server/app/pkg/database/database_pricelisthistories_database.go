package database

import (
	"time"

	"github.com/boltdb/bolt"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/util"
)

func newPricelistHistoryDatabase(dbFilepath string, targetDate time.Time) (PricelistHistoryDatabase, error) {
	db, err := bolt.Open(dbFilepath, 0600, nil)
	if err != nil {
		return PricelistHistoryDatabase{}, err
	}

	return PricelistHistoryDatabase{db, targetDate}, nil
}

type PricelistHistoryDatabase struct {
	db         *bolt.DB
	targetDate time.Time
}

// gathering item-price-histories
type getItemPriceHistoriesJob struct {
	err     error
	ItemID  blizzard.ItemID
	history sotah.PriceHistory
}

func (phdBase PricelistHistoryDatabase) getItemPriceHistories(itemIds []blizzard.ItemID) chan getItemPriceHistoriesJob {
	// drawing channels
	in := make(chan blizzard.ItemID)
	out := make(chan getItemPriceHistoriesJob)

	// spinning up workers
	worker := func() {
		for itemId := range in {
			history, err := phdBase.getItemPriceHistory(itemId)
			out <- getItemPriceHistoriesJob{err, itemId, history}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	// spinning it up
	go func() {
		for _, itemId := range itemIds {
			in <- itemId
		}

		close(in)
	}()

	return out
}

func (phdBase PricelistHistoryDatabase) getItemPriceHistory(itemID blizzard.ItemID) (sotah.PriceHistory, error) {
	out := sotah.PriceHistory{}

	err := phdBase.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(pricelistHistoryBucketName(itemID))
		if bkt == nil {
			return nil
		}

		value := bkt.Get(pricelistHistoryKeyName())
		if value == nil {
			return nil
		}

		var err error
		out, err = sotah.NewPriceHistoryFromBytes(value)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return sotah.PriceHistory{}, err
	}

	return out, nil
}

func (phdBase PricelistHistoryDatabase) persistItemPrices(targetTime time.Time, iPrices sotah.ItemPrices) error {
	targetTimestamp := sotah.UnixTimestamp(targetTime.Unix())

	logging.WithFields(logrus.Fields{
		"target-date": targetTimestamp,
		"item-prices": len(iPrices),
	}).Debug("Writing item-prices")

	ipHistories := sotah.ItemPriceHistories{}
	for job := range phdBase.getItemPriceHistories(iPrices.ItemIds()) {
		if job.err != nil {
			return job.err
		}

		ipHistories[job.ItemID] = job.history
	}

	err := phdBase.db.Batch(func(tx *bolt.Tx) error {
		for ItemID, pricesValue := range iPrices {
			pHistory := func() sotah.PriceHistory {
				result, ok := ipHistories[ItemID]
				if !ok {
					return sotah.PriceHistory{}
				}

				return result
			}()
			pHistory[targetTimestamp] = pricesValue

			bkt, err := tx.CreateBucketIfNotExists(pricelistHistoryBucketName(ItemID))
			if err != nil {
				return err
			}

			encodedValue, err := pHistory.EncodeForPersistence()
			if err != nil {
				return err
			}

			if err := bkt.Put(pricelistHistoryKeyName(), encodedValue); err != nil {
				return err
			}
		}

		logging.WithFields(logrus.Fields{
			"target-date": targetTimestamp,
			"item-prices": len(iPrices),
		}).Debug("Finished writing item-price-histories")

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (phdBase PricelistHistoryDatabase) persistEncodedItemPrices(data map[blizzard.ItemID][]byte) error {
	logging.WithField("items", len(data)).Info("Persisting encoded item-prices")

	err := phdBase.db.Batch(func(tx *bolt.Tx) error {
		for itemId, payload := range data {
			bkt, err := tx.CreateBucketIfNotExists(pricelistHistoryBucketName(itemId))
			if err != nil {
				return err
			}

			if err := bkt.Put(pricelistHistoryKeyName(), payload); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}
