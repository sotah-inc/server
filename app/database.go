package main

import (
	"fmt"
	"time"

	"github.com/boltdb/bolt"
	"github.com/ihsw/sotah-server/app/blizzard"
	log "github.com/sirupsen/logrus"
)

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

type database struct {
	db    *bolt.DB
	realm realm
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

		filteredRealms := sta.Realms.filterWithWhitelist(*wList)
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
					if _, err := tx.CreateBucketIfNotExists([]byte(fmt.Sprintf("item-prices/%d", itemID))); err != nil {
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
