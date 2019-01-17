package main

import (
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/sotah-inc/server/app/logging"
)

func storeDatabasePath(c config) (string, error) {
	dbDir, err := c.databaseDir()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s/store.db", dbDir), nil
}

func newStoreDatabase(c config) (storeDatabase, error) {
	dbFilepath, err := storeDatabasePath(c)
	if err != nil {
		return storeDatabase{}, err
	}

	logging.WithField("filepath", dbFilepath).Info("Initializing store database")

	db, err := bolt.Open(dbFilepath, 0600, nil)
	if err != nil {
		return storeDatabase{}, err
	}

	return storeDatabase{db}, nil
}

type storeDatabase struct {
	db *bolt.DB
}
