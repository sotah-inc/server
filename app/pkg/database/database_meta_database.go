package database

import (
	"github.com/boltdb/bolt"
)

func newMetaDatabase(dbFilepath string) (MetaDatabase, error) {
	db, err := bolt.Open(dbFilepath, 0600, nil)
	if err != nil {
		return MetaDatabase{}, err
	}

	return MetaDatabase{db}, nil
}

type MetaDatabase struct {
	db *bolt.DB
}
