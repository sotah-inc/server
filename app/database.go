package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger"
	badgerOptions "github.com/dgraph-io/badger/options"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/logging"
	"github.com/sirupsen/logrus"
)

func itemPricelistBucketPrefix(rea realm, ID blizzard.ItemID) []byte {
	return []byte(fmt.Sprintf("item-prices/%s/%d", rea.Slug, ID))
}

func itemPricelistBucketName(rea realm, ID blizzard.ItemID, targetDate time.Time) []byte {
	return []byte(fmt.Sprintf("%s/%d", string(itemPricelistBucketPrefix(rea, ID)), targetDate.Unix()))
}

func newDatabase(c config, reg region) (database, error) {
	dbFilepath, err := reg.databaseFilepath(&c)
	if err != nil {
		return database{}, err
	}

	opts := badger.DefaultOptions
	opts.Dir = dbFilepath
	opts.ValueDir = dbFilepath
	opts.ValueLogLoadingMode = badgerOptions.FileIO
	opts.MaxTableSize = 4 << 20 // 4MB
	db, err := badger.Open(opts)
	if err != nil {
		return database{}, err
	}

	return database{db, reg}, nil
}

type priceListHistory map[int64]prices

type database struct {
	db     *badger.DB
	region region
}

func (dBase database) persistPricelists(rea realm, targetDate time.Time, pList priceList) error {
	logging.WithFields(logrus.Fields{
		"region":     dBase.region.Name,
		"realm":      rea.Slug,
		"pricelists": len(pList),
	}).Debug("Writing pricelists")

	err := dBase.db.Update(func(txn *badger.Txn) error {
		for ID, pricesValue := range pList {
			key := itemPricelistBucketName(rea, ID, targetDate)

			encodedPricesValue, err := json.Marshal(pricesValue)
			if err != nil {
				return err
			}

			txn.Set(key, encodedPricesValue)
		}

		logging.WithFields(logrus.Fields{
			"region":     dBase.region.Name,
			"realm":      rea.Slug,
			"pricelists": len(pList),
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
	err := dBase.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := itemPricelistBucketPrefix(rea, ID)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			itItem := it.Item()
			k := itItem.Key()
			v, err := itItem.Value()
			if err != nil {
				return err
			}

			s := strings.Split(string(k), "/")
			targetTimeUnix, err := strconv.Atoi(s[3])
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

func newDatabases(c config) (databases, error) {
	dbs := databases{}
	for _, reg := range c.Regions {
		// gathering whitelist for this region
		wList := c.getRegionWhitelist(reg.Name)
		if wList != nil && len(*wList) == 0 {
			continue
		}

		dBase, err := newDatabase(c, reg)
		if err != nil {
			return databases{}, err
		}

		dbs[reg.Name] = dBase
	}

	return dbs, nil
}

type databases map[regionName]database
