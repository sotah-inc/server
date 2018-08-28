package main

import (
	"time"

	"github.com/ihsw/sotah-server/app/blizzard"

	"github.com/boltdb/bolt"
)

func newDatabase(c config, rea realm) (database, error) {
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

func newDatabases(c config, stas statuses) (databases, error) {
	dbs := databases{}
	for rName, sta := range stas {
		dbs[rName] = map[blizzard.RealmSlug]database{}
		for _, rea := range sta.Realms {
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
