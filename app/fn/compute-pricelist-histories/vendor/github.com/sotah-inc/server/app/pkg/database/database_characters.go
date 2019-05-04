package database

import (
	"fmt"

	"github.com/sotah-inc/server/app/pkg/blizzard"
)

// bucketing
func databaseCharactersBucketName() []byte {
	return []byte("characters")
}

// keying
func charactersKeyName(name string) []byte {
	return []byte(fmt.Sprintf("character-%s", name))
}

// db
func charactersDatabaseFilePath(
	dbDir string,
	regionName blizzard.RegionName,
	realmSlug blizzard.RealmSlug,
) (string, error) {
	return fmt.Sprintf("%s/characters/%s/%s.db", dbDir, regionName, realmSlug), nil
}
