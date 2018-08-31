package main

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
	"github.com/ihsw/sotah-server/app/blizzard"
	log "github.com/sirupsen/logrus"
)

func itemIDPricelistBucketName(ID blizzard.ItemID) []byte {
	return []byte(fmt.Sprintf("item-prices/%d", ID))
}

func newDatabase(c config, rea realm, itemIds []blizzard.ItemID) (database, error) {
	dbFilepath, err := rea.databaseFilepath(&c)
	if err != nil {
		return database{}, err
	}

	db, err := bolt.Open(dbFilepath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return database{}, err
	}

	return database{db, rea}, nil
}

type priceListHistory map[int64]priceList

type database struct {
	db    *bolt.DB
	realm realm
}

func (dBase database) getPricelistHistory(ID blizzard.ItemID) (priceListHistory, error) {
	plHistory := map[int64]priceList{}
	err := dBase.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(itemIDPricelistBucketName(ID))
		if b == nil {
			return nil
		}

		b.ForEach(func(k, v []byte) error {
			unixTime := int64(binary.BigEndian.Uint64(k))
			pList, err := newPriceListFromBytes(v)
			if err != nil {
				return err
			}

			plHistory[unixTime] = pList

			return nil
		})

		return nil
	})
	if err != nil {
		return priceListHistory{}, err
	}

	return plHistory, nil
}

func newDatabases(c config, stas statuses, itemIds []blizzard.ItemID) (databases, error) {
	dbs := databases{}
	for rName, sta := range stas {
		// misc
		dbs[rName] = map[blizzard.RealmSlug]database{}

		// gathering whitelist for this region
		wList := c.getRegionWhitelist(rName)
		if wList != nil && len(*wList) == 0 {
			continue
		}

		filteredRealms := sta.Realms.filterWithWhitelist(wList)
		log.WithField("count", len(filteredRealms)).Info("Initializing databases")
		for _, rea := range filteredRealms {
			dBase, err := newDatabase(c, rea, itemIds)
			if err != nil {
				return databases{}, err
			}

			log.WithFields(log.Fields{
				"region": rName,
				"realm":  rea.Slug,
				"count":  len(itemIds),
			}).Info("Ensuring item-price buckets exist")
			err = dBase.db.Batch(func(tx *bolt.Tx) error {
				for _, itemID := range itemIds {
					if _, err := tx.CreateBucketIfNotExists(itemIDPricelistBucketName(itemID)); err != nil {
						return err
					}

					return nil
				}

				return nil
			})
			if err != nil {
				return databases{}, err
			}

			dbs[rName][rea.Slug] = dBase
		}
	}

	return dbs, nil
}

type databases map[regionName]map[blizzard.RealmSlug]database
