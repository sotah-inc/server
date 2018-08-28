package main

import (
	"time"

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
