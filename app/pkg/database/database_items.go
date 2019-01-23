package database

import (
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/sotah-inc/server/app/internal"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
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

func itemsDatabasePath(c internal.Config) (string, error) {
	dbDir, err := c.DatabaseDir()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s/items.db", dbDir), nil
}

func NewItemsDatabase(c internal.Config) (ItemsDatabase, error) {
	dbFilepath, err := itemsDatabasePath(c)
	if err != nil {
		return ItemsDatabase{}, err
	}

	logging.WithField("filepath", dbFilepath).Info("Initializing items database")

	db, err := bolt.Open(dbFilepath, 0600, nil)
	if err != nil {
		return ItemsDatabase{}, err
	}

	return ItemsDatabase{db}, nil
}

type ItemsDatabase struct {
	db *bolt.DB
}

func (idBase ItemsDatabase) GetItems() (internal.ItemsMap, error) {
	out := internal.ItemsMap{}

	err := idBase.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(databaseItemsBucketName())
		if bkt == nil {
			return nil
		}

		err := bkt.ForEach(func(k, v []byte) error {
			iMap, err := internal.NewItemsMapFromGzipped(v)
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
		return internal.ItemsMap{}, err
	}

	return out, nil
}

func (idBase ItemsDatabase) FindItems(IDs []blizzard.ItemID) (internal.ItemsMap, error) {
	keyspaces := func() []itemKeyspace {
		result := map[itemKeyspace]struct{}{}
		for _, ID := range IDs {
			result[itemIDKeyspace(ID)] = struct{}{}
		}

		out := []itemKeyspace{}
		for keyspace := range result {
			out = append(out, keyspace)
		}

		return out
	}()

	IDsMap := internal.NewItemIdsMap(IDs)
	out := internal.ItemsMap{}
	err := idBase.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(databaseItemsBucketName())
		if bkt == nil {
			return nil
		}

		for _, keyspace := range keyspaces {
			value := bkt.Get(itemsKeyName(keyspace))
			if value == nil {
				continue
			}

			iMap, err := internal.NewItemsMapFromGzipped(value)
			if err != nil {
				return err
			}

			for ID, itemValue := range iMap {
				if _, ok := IDsMap[ID]; !ok {
					continue
				}

				out[ID] = itemValue
			}
		}

		return nil
	})
	if err != nil {
		return internal.ItemsMap{}, err
	}

	return out, nil
}

func newItemsMapBatch(iMap internal.ItemsMap) itemsMapBatch {
	imBatch := itemsMapBatch{}
	for ID, itemValue := range iMap {
		keyspace := itemIDKeyspace(ID)
		if _, ok := imBatch[keyspace]; !ok {
			imBatch[keyspace] = internal.ItemsMap{ID: itemValue}
		} else {
			imBatch[keyspace][ID] = itemValue
		}
	}

	return imBatch
}

type itemKeyspace int64

type itemsMapBatch map[itemKeyspace]internal.ItemsMap

func (idBase ItemsDatabase) PersistItems(iMap internal.ItemsMap) error {
	logging.WithField("items", len(iMap)).Debug("Persisting items")

	imBatch := newItemsMapBatch(iMap)

	logging.WithField("batches", len(imBatch)).Debug("Persisting batches")

	err := idBase.db.Batch(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists(databaseItemsBucketName())
		if err != nil {
			return err
		}

		for keyspace, batchMap := range imBatch {
			encodedItemsMap, err := batchMap.EncodeForDatabase()
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
