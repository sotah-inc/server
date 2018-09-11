package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/logging"
	"github.com/sirupsen/logrus"
)

func itemPricelistBucketPrefix(ID blizzard.ItemID) []byte {
	return []byte(fmt.Sprintf("item-prices/%d", ID))
}

func itemPricelistBucketName(ID blizzard.ItemID, targetDate time.Time) []byte {
	return []byte(fmt.Sprintf("%s/%d", itemPricelistBucketPrefix(ID), targetDate.Unix()))
}

func newDatabase(c config, rea realm) (database, error) {
	dbFilepath, err := rea.databaseFilepath(&c)
	if err != nil {
		return database{}, err
	}

	opts := badger.DefaultOptions
	opts.Dir = dbFilepath
	opts.ValueDir = dbFilepath
	db, err := badger.Open(opts)
	if err != nil {
		return database{}, err
	}

	return database{db, rea}, nil
}

type priceListHistory map[int64]prices

type database struct {
	db    *badger.DB
	realm realm
}

func (dBase database) persistPricelists(targetDate time.Time, pList priceList) error {
	logging.WithFields(logrus.Fields{
		"region":     dBase.realm.region.Name,
		"realm":      dBase.realm.Slug,
		"pricelists": len(pList),
	}).Info("Writing pricelists")

	return dBase.db.Update(func(txn *badger.Txn) error {
		for ID, pricesValue := range pList {
			key := itemPricelistBucketName(ID, targetDate)

			encodedPricesValue, err := json.Marshal(pricesValue)
			if err != nil {
				return err
			}

			txn.Set(key, encodedPricesValue)
		}

		return nil
	})
}

func (dBase database) getPricelistHistory(ID blizzard.ItemID) (priceListHistory, error) {
	plHistory := priceListHistory{}
	err := dBase.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := itemPricelistBucketPrefix(ID)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			itItem := it.Item()
			k := itItem.Key()
			v, err := itItem.Value()
			if err != nil {
				return err
			}

			s := strings.Split(string(k), "/")
			targetTimeUnix, err := strconv.Atoi(s[2])
			if err != nil {
				return err
			}

			p, err := newPricesFromBytes(v)
			if err != nil {
				return err
			}
			plHistory[int64(targetTimeUnix)] = p
		}

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
		logging.WithField("count", len(filteredRealms)).Info("Initializing databases")
		for _, rea := range filteredRealms {
			dBase, err := newDatabase(c, rea)
			if err != nil {
				return databases{}, err
			}

			dbs[rName][rea.Slug] = dBase
		}
	}

	return dbs, nil
}

type databases map[regionName]map[blizzard.RealmSlug]database
