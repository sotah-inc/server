package main

import (
	"encoding/binary"
	"fmt"

	"github.com/ihsw/sotah-server/app/blizzard"

	"github.com/boltdb/bolt"
)

func itemsBucketName() []byte {
	return []byte("items")
}

func itemsKeyName(ID blizzard.ItemID) []byte {
	keyspaceSize := int64(1000)
	keyspace := (int64(ID) - (int64(ID) % keyspaceSize)) / keyspaceSize

	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, uint64(keyspace))

	return key
}

func itemsDatabasePath(c config) (string, error) {
	dbDir, err := c.databaseDir()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s/items.db", dbDir), nil
}

func newItemsDatabase(c config) (itemsDatabase, error) {
	dbFilepath, err := itemsDatabasePath(c)
	if err != nil {
		return itemsDatabase{}, err
	}

	db, err := bolt.Open(dbFilepath, 0600, nil)
	if err != nil {
		return itemsDatabase{}, err
	}

	return itemsDatabase{db}, nil
}

type itemsDatabase struct {
	db *bolt.DB
}
