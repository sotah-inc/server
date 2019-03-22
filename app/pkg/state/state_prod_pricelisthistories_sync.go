package state

import (
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/store"
)

func (phState ProdPricelistHistoriesState) Sync() error {
	// gathering region-realms
	regionRealms := map[blizzard.RegionName]sotah.Realms{}
	for regionName, status := range phState.Statuses {
		regionRealms[regionName] = status.Realms
	}

	// gathering existing pricelist-histories versions
	versions, err := phState.PricelistHistoriesBase.GetVersions(regionRealms, phState.PricelistHistoriesBucket)
	if err != nil {
		return err
	}

	// trimming matching versions
	versionsToSync := store.PricelistHistoryVersions{}
	for regionName, realmTimestampVersions := range versions {
		for realmSlug, timestampVersions := range realmTimestampVersions {
			for targetTimestamp, version := range timestampVersions {
				hasBucket, err := phState.IO.Databases.MetaDatabase.HasBucket(regionName, realmSlug)
				if err != nil {
					return err
				}

				if !hasBucket {
					versionsToSync = versionsToSync.Insert(regionName, realmSlug, targetTimestamp, version)

					continue
				}

				hasVersion, err := phState.IO.Databases.MetaDatabase.HasPricelistHistoriesVersion(
					regionName,
					realmSlug,
					targetTimestamp,
				)
				if err != nil {
					return err
				}

				if !hasVersion {
					versionsToSync = versionsToSync.Insert(regionName, realmSlug, targetTimestamp, version)

					continue
				}

				currentVersion, err := phState.IO.Databases.MetaDatabase.GetPricelistHistoriesVersion(
					regionName,
					realmSlug,
					targetTimestamp,
				)
				if err != nil {
					return err
				}

				if currentVersion != version {
					versionsToSync = versionsToSync.Insert(regionName, realmSlug, targetTimestamp, version)

					continue
				}
			}
		}
	}

	logging.WithField("versions", versionsToSync).Info("Found versions to sync")

	return nil
}
