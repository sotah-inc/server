package database

import (
	"fmt"
	"strconv"

	"github.com/sotah-inc/server/app/pkg/blizzard"
)

// bucketing
func databaseItemsBucketName() []byte {
	return []byte("items")
}

func databaseItemNamesBucketName() []byte {
	return []byte("item-names")
}

// keying
func itemsKeyName(id blizzard.ItemID) []byte {
	return []byte(fmt.Sprintf("item-%d", id))
}

func itemNameKeyName(id blizzard.ItemID) []byte {
	return []byte(fmt.Sprintf("item-name-%d", id))
}

func itemIdFromItemNameKeyName(key []byte) (blizzard.ItemID, error) {
	unparsedItemId, err := strconv.Atoi(string(key)[len("item-name-"):])
	if err != nil {
		return blizzard.ItemID(0), err
	}

	return blizzard.ItemID(unparsedItemId), nil
}

// db
func itemsDatabasePath(dbDir string) (string, error) {
	return fmt.Sprintf("%s/items.db", dbDir), nil
}
