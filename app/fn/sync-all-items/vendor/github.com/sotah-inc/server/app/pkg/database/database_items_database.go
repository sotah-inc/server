package database

import (
	"github.com/sotah-inc/server/app/pkg/sotah"

	"github.com/boltdb/bolt"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
)

func NewItemsDatabase(dbDir string) (ItemsDatabase, error) {
	dbFilepath, err := itemsDatabasePath(dbDir)
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

// gathering items
func (idBase ItemsDatabase) GetItems() (sotah.ItemsMap, error) {
	out := sotah.ItemsMap{}

	err := idBase.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(databaseItemsBucketName())
		if bkt == nil {
			return nil
		}

		err := bkt.ForEach(func(k, v []byte) error {
			iMap, err := sotah.NewItemsMapFromGzipped(v)
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
		return sotah.ItemsMap{}, err
	}

	return out, nil
}

func (idBase ItemsDatabase) FindItems(itemIds []blizzard.ItemID) (sotah.ItemsMap, error) {
	// gathering item keyspaces for fetching
	keyspaces := func() []itemKeyspace {
		result := map[itemKeyspace]struct{}{}
		for _, ID := range itemIds {
			result[itemIDKeyspace(ID)] = struct{}{}
		}

		out := []itemKeyspace{}
		for keyspace := range result {
			out = append(out, keyspace)
		}

		return out
	}()

	// producing an id map for simpler filtering of results
	itemIdsMap := sotah.NewItemIdsMap(itemIds)

	out := sotah.ItemsMap{}
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

			iMap, err := sotah.NewItemsMapFromGzipped(value)
			if err != nil {
				return err
			}

			for itemId, item := range iMap {
				if _, ok := itemIdsMap[itemId]; !ok {
					continue
				}

				out[itemId] = item
			}
		}

		return nil
	})
	if err != nil {
		return sotah.ItemsMap{}, err
	}

	return out, nil
}

// persisting
func newItemsMapBatch(iMap sotah.ItemsMap) itemsMapBatch {
	out := itemsMapBatch{}
	for ID, itemValue := range iMap {
		keyspace := itemIDKeyspace(ID)
		if _, ok := out[keyspace]; !ok {
			out[keyspace] = sotah.ItemsMap{ID: itemValue}

			continue
		} else {
			out[keyspace][ID] = itemValue
		}
	}

	return out
}

type itemsMapBatch map[itemKeyspace]sotah.ItemsMap

func (idBase ItemsDatabase) PersistItems(iMap sotah.ItemsMap) error {
	logging.WithField("items", len(iMap)).Debug("Persisting items")

	// grouping items into batches based on keyspace
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

func (idBase ItemsDatabase) FilterInItemsToSync(ids blizzard.ItemIds) (blizzard.ItemIds, error) {
	out := blizzard.ItemIds{}
	err := idBase.db.View(func(tx *bolt.Tx) error {
		return nil
	})
	if err != nil {
		return blizzard.ItemIds{}, err
	}

	return out, nil
}
