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
func itemsKeyName(id blizzard.ItemID) []byte {
	return []byte(fmt.Sprintf("item-%d", id))
}

// db
func itemsDatabasePath(dbDir string) (string, error) {
	return fmt.Sprintf("%s/items.db", dbDir), nil
}
