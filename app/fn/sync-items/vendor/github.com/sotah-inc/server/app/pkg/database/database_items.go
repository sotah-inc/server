package database

import (
	"fmt"

	"github.com/sotah-inc/server/app/pkg/blizzard"
)

// bucketing
func databaseItemsBucketName() []byte {
	return []byte("items")
}

// keying
type itemKeyspace int64

func itemIDKeyspace(itemId blizzard.ItemID) itemKeyspace {
	keyspaceSize := int64(1000)
	keyspace := (int64(itemId) - (int64(itemId) % keyspaceSize)) / keyspaceSize

	return itemKeyspace(keyspace)
}

func itemsKeyName(keyspace itemKeyspace) []byte {
	return []byte(fmt.Sprintf("item-batch-%d", keyspace))
}

// db
func itemsDatabasePath(dbDir string) (string, error) {
	return fmt.Sprintf("%s/items.db", dbDir), nil
}
