package database

import (
	"encoding/base64"
	"encoding/json"
	"strconv"

	"github.com/boltdb/bolt"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/util"
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
			parsedId, err := strconv.Atoi(string(k)[len("item-"):])
			if err != nil {
				return err
			}
			itemId := blizzard.ItemID(parsedId)

			gzipDecoded, err := util.GzipDecode(v)
			if err != nil {
				return err
			}

			item, err := sotah.NewItem(gzipDecoded)
			if err != nil {
				return err
			}

			out[itemId] = item

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
	out := sotah.ItemsMap{}
	err := idBase.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(databaseItemsBucketName())
		if bkt == nil {
			return nil
		}

		for _, id := range itemIds {
			value := bkt.Get(itemsKeyName(id))
			if value == nil {
				continue
			}

			gzipDecoded, err := util.GzipDecode(value)
			if err != nil {
				return err
			}

			item, err := sotah.NewItem(gzipDecoded)
			if err != nil {
				return err
			}

			out[id] = item
		}

		return nil
	})
	if err != nil {
		return sotah.ItemsMap{}, err
	}

	return out, nil
}

// persisting
func (idBase ItemsDatabase) PersistItems(iMap sotah.ItemsMap) error {
	logging.WithField("items", len(iMap)).Debug("Persisting items")

	err := idBase.db.Batch(func(tx *bolt.Tx) error {
		itemsBucket, err := tx.CreateBucketIfNotExists(databaseItemsBucketName())
		if err != nil {
			return err
		}

		itemNamesBucket, err := tx.CreateBucketIfNotExists(databaseItemNamesBucketName())
		if err != nil {
			return err
		}

		for id, item := range iMap {
			jsonEncoded, err := json.Marshal(item)
			if err != nil {
				return err
			}

			gzipEncoded, err := util.GzipEncode(jsonEncoded)
			if err != nil {
				return err
			}

			if err := itemsBucket.Put(itemsKeyName(id), gzipEncoded); err != nil {
				return err
			}

			normalizedName, err := sotah.NormalizeName(item.NormalizedName)
			if err != nil {
				return err
			}

			if err := itemNamesBucket.Put(itemNameKeyName(id), []byte(normalizedName)); err != nil {
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

type ReceiveSyncedItemsData struct {
	ItemIdNamesMap map[blizzard.ItemID]string
}

type PersistEncodedItemsInJob struct {
	Id              blizzard.ItemID
	GzipEncodedData []byte
}

func (idBase ItemsDatabase) PersistEncodedItems(
	in chan PersistEncodedItemsInJob,
	idNameMap sotah.ItemIdNameMap,
) error {
	logging.Info("Persisting encoded items")

	err := idBase.db.Batch(func(tx *bolt.Tx) error {
		itemsBucket, err := tx.CreateBucketIfNotExists(databaseItemsBucketName())
		if err != nil {
			return err
		}

		itemNamesBucket, err := tx.CreateBucketIfNotExists(databaseItemNamesBucketName())
		if err != nil {
			return err
		}

		for job := range in {
			if err := itemsBucket.Put(itemsKeyName(job.Id), job.GzipEncodedData); err != nil {
				return err
			}
		}

		i := 0
		for id, normalizedName := range idNameMap {
			if err := itemNamesBucket.Put(itemNameKeyName(id), []byte(normalizedName)); err != nil {
				return err
			}

			if i%100 == 0 {
				logging.WithFields(logrus.Fields{
					"id":   id,
					"name": normalizedName,
				}).Info("Inserted into item-names bucket")
			}

			i++
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func NewItemsSyncPayload(data string) (ItemsSyncPayload, error) {
	base64Decoded, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return ItemsSyncPayload{}, err
	}

	gzipDecoded, err := util.GzipDecode(base64Decoded)
	if err != nil {
		return ItemsSyncPayload{}, err
	}

	var out ItemsSyncPayload
	if err := json.Unmarshal(gzipDecoded, &out); err != nil {
		return ItemsSyncPayload{}, err
	}

	return out, nil
}

type ItemsSyncPayload struct {
	Ids        blizzard.ItemIds
	IconIdsMap map[string]blizzard.ItemIds
}

func (p ItemsSyncPayload) EncodeForDelivery() (string, error) {
	jsonEncoded, err := json.Marshal(p)
	if err != nil {
		return "", err
	}

	gzipEncoded, err := util.GzipEncode(jsonEncoded)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(gzipEncoded), nil
}

func (idBase ItemsDatabase) FilterInItemsToSync(ids blizzard.ItemIds) (ItemsSyncPayload, error) {
	// producing a blank whitelist
	syncWhitelist := map[blizzard.ItemID]bool{}
	for _, id := range ids {
		syncWhitelist[id] = false
	}

	// producing a blank map of icon->item-ids
	iconsToSync := map[string]blizzard.ItemIds{}

	// peeking into the items database
	err := idBase.db.Update(func(tx *bolt.Tx) error {
		itemsBucket, err := tx.CreateBucketIfNotExists(databaseItemsBucketName())
		if err != nil {
			return err
		}

		itemNamesBucket, err := tx.CreateBucketIfNotExists(databaseItemNamesBucketName())
		if err != nil {
			return err
		}

		for _, id := range ids {
			value := itemsBucket.Get(itemsKeyName(id))
			if value == nil {
				logging.WithField("item", id).Info("Item was not in bucket")
				syncWhitelist[id] = true

				continue
			}

			gzipDecoded, err := util.GzipDecode(value)
			if err != nil {
				return err
			}

			item, err := sotah.NewItem(gzipDecoded)
			if err != nil {
				return err
			}

			if item.Icon != "" && (item.IconURL == "" || item.IconObjectName == "") {
				iconItemIds := func() blizzard.ItemIds {
					out, ok := iconsToSync[item.Icon]
					if !ok {
						return blizzard.ItemIds{}
					}

					return out
				}()
				iconItemIds = append(iconItemIds, id)
				iconsToSync[item.Icon] = iconItemIds
			}

			if item.NormalizedName == "" {
				logging.WithField("item", item.ID).Info("Normalized-name is blank")
				syncWhitelist[id] = true
			}

			normalizedNameValue := itemNamesBucket.Get(itemNameKeyName(id))
			if normalizedNameValue == nil {
				logging.WithField("item", item.ID).Info("Normalized-name not in bucket")
				syncWhitelist[id] = true
			} else {
				if string(normalizedNameValue) == "" {
					logging.WithField("item", item.ID).Info("Normalized-name was a blank string")
					syncWhitelist[id] = true
				} else {
					if string(normalizedNameValue) != item.NormalizedName {
						logging.WithField("item", item.ID).Info("Normalized-name did not match item normalized-name")
						syncWhitelist[id] = true
					}
				}
			}
		}

		return nil
	})
	if err != nil {
		return ItemsSyncPayload{}, err
	}

	// reformatting the whitelist
	idsToSync := blizzard.ItemIds{}
	for id, shouldSync := range syncWhitelist {
		if !shouldSync {
			continue
		}

		idsToSync = append(idsToSync, id)
	}

	return ItemsSyncPayload{Ids: idsToSync, IconIdsMap: iconsToSync}, nil
}
