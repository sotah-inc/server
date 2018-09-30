package main

import (
	"fmt"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/logging"

	"github.com/boltdb/bolt"
)

func databaseItemsBucketName() []byte {
	return []byte("items")
}

func itemIDKeyspace(ID blizzard.ItemID) itemKeyspace {
	keyspaceSize := int64(1000)
	keyspace := (int64(ID) - (int64(ID) % keyspaceSize)) / keyspaceSize

	return itemKeyspace(keyspace)
}

func itemsKeyName(keyspace itemKeyspace) []byte {
	return []byte(fmt.Sprintf("item-batch-%d", keyspace))
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

	logging.WithField("filepath", dbFilepath).Info("Initializing items database")

	db, err := bolt.Open(dbFilepath, 0600, nil)
	if err != nil {
		return itemsDatabase{}, err
	}

	return itemsDatabase{db}, nil
}

type itemsDatabase struct {
	db *bolt.DB
}

func (idBase itemsDatabase) getItems() (itemsMap, error) {
	out := itemsMap{}

	err := idBase.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(databaseItemsBucketName())
		if bkt == nil {
			return nil
		}

		err := bkt.ForEach(func(k, v []byte) error {
			iMap, err := newItemsMapFromGzipped(v)
			if err != nil {
				return err
			}

			for ID, itemValue := range iMap {
				out[ID] = itemValue
			}

			return nil
		})
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return itemsMap{}, err
	}

	return out, nil
}

func newItemsMapBatch(iMap itemsMap) itemsMapBatch {
	imBatch := itemsMapBatch{}
	for ID, itemValue := range iMap {
		keyspace := itemIDKeyspace(ID)
		if _, ok := imBatch[keyspace]; !ok {
			imBatch[keyspace] = itemsMap{ID: itemValue}
		} else {
			imBatch[keyspace][ID] = itemValue
		}
	}

	return imBatch
}

type itemKeyspace int64

type itemsMapBatch map[itemKeyspace]itemsMap

func (idBase itemsDatabase) persistItems(iMap itemsMap) error {
	logging.WithField("items", len(iMap)).Debug("Persisting items")

	imBatch := newItemsMapBatch(iMap)

	logging.WithField("batches", len(imBatch)).Debug("Persisting batches")

	err := idBase.db.Batch(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists(databaseItemsBucketName())
		if err != nil {
			return err
		}

		for keyspace, batchMap := range imBatch {
			encodedItemsMap, err := batchMap.encodeForDatabase()
			if err != nil {
				return err
			}

			if err := bkt.Put(itemsKeyName(keyspace), encodedItemsMap); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}
