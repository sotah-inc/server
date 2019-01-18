package main

import (
	"encoding/json"
	"fmt"
	"github.com/sotah-inc/server/app/util"

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

func (sdBase storeDatabase) bucketName(reg region) []byte {
	return []byte(reg.Name)
}

func (sdBase storeDatabase) keyName(rea realm) []byte {
	return []byte(rea.Slug)
}

func newStoreDatabaseDataFromBytes(data []byte) (storeDatabaseData, error) {
	gzipDecoded, err := util.GzipDecode(data)
	if err != nil {
		return storeDatabaseData{}, err
	}

	out := storeDatabaseData{}
	if err := json.Unmarshal(gzipDecoded, &out); err != nil {
		return storeDatabaseData{}, err
	}

	return out, nil
}

type storeDatabaseData struct {
	data map[unixTimestamp]storeDatabaseDataItem
}

func (sd storeDatabaseData) encodeForPersistence() ([]byte, error) {
	jsonEncoded, err := json.Marshal(sd)
	if err != nil {
		return []byte{}, err
	}

	gzipEncoded, err := util.GzipEncode(jsonEncoded)
	if err != nil {
		return []byte{}, err
	}

	return gzipEncoded, nil
}

type storeDatabaseDataItem struct {
	objectSize    int
	totalAuctions int
	totalOwners   int
}

func (sdBase storeDatabase) getItems(reg region, rea realm) (storeDatabaseData, error) {
	out := storeDatabaseData{}
	err := sdBase.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(sdBase.bucketName(reg))
		if bkt == nil {
			return nil
		}

		value := bkt.Get(sdBase.keyName(rea))
		if value == nil {
			return nil
		}

		var err error
		out, err = newStoreDatabaseDataFromBytes(value)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return storeDatabaseData{}, err
	}

	return out, nil
}

func (sdBase storeDatabase) persistItem(reg region, rea realm, dateOccurred unixTimestamp, sdItem storeDatabaseDataItem) error {
	// resolving the store-database items
	sdItems, err := sdBase.getItems(reg, rea)
	if err != nil {
		return err
	}

	// appending the value to the list of items and encoding it
	sdItems.data[dateOccurred] = sdItem
	encodedData, err := sdItems.encodeForPersistence()
	if err != nil {
		return err
	}

	// persisting the value to the database
	key := sdBase.bucketName(reg)
	return sdBase.db.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists(key)
		if err != nil {
			return err
		}

		return bkt.Put(key, encodedData)
	})
}
