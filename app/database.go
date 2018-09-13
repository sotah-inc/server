package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/boltdb/bolt"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/logging"
	"github.com/sirupsen/logrus"
)

func targetDateToKeyName(targetDate time.Time) []byte {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, uint64(targetDate.Unix()))

	return key
}

func keyNameToTargetDate(key []byte) time.Time {
	return time.Unix(int64(binary.LittleEndian.Uint64(key)), 0)
}

func itemPricelistBucketName(ID blizzard.ItemID) []byte {
	return []byte(fmt.Sprintf("item-prices/%d", ID))
}

func databasePath(c config, reg region, rea realm, targetDate time.Time) (string, error) {
	normalizedUnixTimestamp := int(targetDate.Unix()) - targetDate.Second() - targetDate.Minute()*60 - targetDate.Hour()*60*24

	return filepath.Abs(
		fmt.Sprintf("%s/databases/%s/%s/%d.db", c.CacheDir, reg.Name, rea.Slug, normalizedUnixTimestamp),
	)
}

func newDatabase(c config, reg region, rea realm, targetDate time.Time) (database, error) {
	dbFilepath, err := databasePath(c, reg, rea, targetDate)
	if err != nil {
		return database{}, err
	}

	db, err := bolt.Open(dbFilepath, 0600, nil)
	if err != nil {
		return database{}, err
	}

	return database{db, targetDate}, nil
}

type priceListHistory map[int64]prices

type database struct {
	db         *bolt.DB
	targetDate time.Time
}

func (dBase database) persistPricelists(pList priceList) error {
	logging.WithFields(logrus.Fields{
		"target_date": dBase.targetDate.Unix(),
		"pricelists":  len(pList),
	}).Debug("Writing pricelists")

	err := dBase.db.Batch(func(tx *bolt.Tx) error {
		for ID, pricesValue := range pList {
			bkt, err := tx.CreateBucketIfNotExists(itemPricelistBucketName(ID))
			if err != nil {
				return err
			}

			encodedPricesValue, err := json.Marshal(pricesValue)
			if err != nil {
				return err
			}

			return bkt.Put(targetDateToKeyName(dBase.targetDate), encodedPricesValue)
		}

		logging.WithFields(logrus.Fields{
			"target_date": dBase.targetDate.Unix(),
			"pricelists":  len(pList),
		}).Debug("Finished writing pricelists")

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (dBase database) getPricelistHistory(rea realm, ID blizzard.ItemID) (priceListHistory, error) {
	plHistory := priceListHistory{}
	err := dBase.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(itemPricelistBucketName(ID))
		if bkt == nil {
			return nil
		}

		return bkt.ForEach(func(k, v []byte) error {
			targetDate := keyNameToTargetDate(k)
			pricesValue, err := newPricesFromBytes(v)
			if err != nil {
				return err
			}

			plHistory[targetDate.Unix()] = pricesValue

			return nil
		})
	})
	if err != nil {
		return priceListHistory{}, err
	}

	return plHistory, nil
}

type databases map[regionName]map[blizzard.RealmSlug]map[int64]database
