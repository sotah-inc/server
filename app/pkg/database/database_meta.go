package database

import (
	"fmt"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/sotah"
)

// keying
func metaKeyName(name string) []byte {
	return []byte(name)
}

func metaPricelistHistoryVersionKeyName(targetTimestamp sotah.UnixTimestamp) []byte {
	return metaKeyName(fmt.Sprintf("pricelist-histories/%d", targetTimestamp))
}

// bucketing
func metaBucketName(regionName blizzard.RegionName, realmSlug blizzard.RealmSlug) []byte {
	return []byte(fmt.Sprintf("%s/%s", regionName, realmSlug))
}

// db
func metaDatabaseFilePath(dirPath string) string {
	return fmt.Sprintf("%s/meta.db", dirPath)
}
