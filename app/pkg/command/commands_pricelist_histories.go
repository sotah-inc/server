package command

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/state"

	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/store"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/internal"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
	"github.com/sotah-inc/server/app/pkg/util"
)

func pricelistHistoriesCacheDirs(c internal.Config, regions internal.RegionList, stas internal.Statuses) ([]string, error) {
	databaseDir, err := c.DatabaseDir()
	if err != nil {
		return nil, err
	}

	cacheDirs := []string{databaseDir}
	for _, reg := range regions {
		regionDatabaseDir := reg.DatabaseDir(databaseDir)
		cacheDirs = append(cacheDirs, regionDatabaseDir)

		for _, rea := range stas[reg.Name].Realms {
			cacheDirs = append(cacheDirs, rea.DatabaseDir(regionDatabaseDir))
		}
	}

	return cacheDirs, nil
}

func pricelistHistories(c internal.Config, m messenger.Messenger, s store.Store) error {
	logging.Info("Starting pricelist-histories")

	// establishing a state
	res := internal.NewResolver(c, m, s)
	sta := state.NewState(m, res)

	// gathering region-status from the root service
	logging.Info("Gathering regions")
	regions, err := func() (internal.RegionList, error) {
		out := internal.RegionList{}
		attempts := 0
		for {
			var err error
			out, err = internal.NewRegionsFromMessenger(m)
			if err == nil {
				break
			} else {
				logging.Info("Could not fetch regions, retrying in 250ms")

				attempts++
				time.Sleep(250 * time.Millisecond)
			}

			if attempts >= 20 {
				return internal.RegionList{}, fmt.Errorf("Failed to fetch regions after %d attempts", attempts)
			}
		}

		return out, nil
	}()
	if err != nil {
		logging.WithField("error", err.Error()).Error("Failed to fetch regions")

		return err
	}

	sta.Regions = c.FilterInRegions(regions)

	// filling state with statuses
	logging.Info("Gathering statuses")
	for _, reg := range sta.Regions {
		regionStatus, err := internal.NewStatusFromMessenger(reg, m)
		if err != nil {
			logging.WithField("region", reg.Name).Info("Could not fetch status for region")

			return err
		}
		regionStatus.Realms = c.FilterInRealms(reg, regionStatus.Realms)
		sta.Statuses[reg.Name] = regionStatus
	}

	// ensuring cache-dirs exist
	logging.Info("Ensuring cache-dirs exist")
	cacheDirs, err := pricelistHistoriesCacheDirs(c, sta.Regions, sta.Statuses)
	if err != nil {
		return err
	}

	if err := util.EnsureDirsExist(cacheDirs); err != nil {
		return err
	}

	// pruning old data
	databaseDir, err := c.DatabaseDir()
	if err != nil {
		return err
	}

	earliestTime := database.DatabaseRetentionLimit()
	for _, reg := range sta.Regions {
		regionDatabaseDir := reg.DatabaseDir(databaseDir)

		for _, rea := range sta.Statuses[reg.Name].Realms {
			realmDatabaseDir := rea.DatabaseDir(regionDatabaseDir)
			dbPaths, err := database.DatabasePaths(realmDatabaseDir)
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error": err.Error(),
					"dir":   realmDatabaseDir,
				}).Error("Failed to resolve database paths")

				return err
			}
			for _, dbPathPair := range dbPaths {
				if dbPathPair.TargetTime.After(earliestTime) {
					continue
				}

				logging.WithFields(logrus.Fields{
					"pathname": dbPathPair.FullPath,
				}).Debug("Pruning old pricelist-history database file")

				if err := os.Remove(dbPathPair.FullPath); err != nil {
					logging.WithFields(logrus.Fields{
						"error":    err.Error(),
						"dir":      realmDatabaseDir,
						"pathname": dbPathPair.FullPath,
					}).Error("Failed to remove database file")

					return err
				}
			}
		}
	}

	// loading up databases
	logging.Info("Loading up databases")
	phdBases, err := database.NewPricelistHistoryDatabases(c, sta.Regions, sta.Statuses)
	if err != nil {
		logging.WithField("error", err.Error()).Error("Failed to load databases")

		return err
	}
	sta.PricelistHistoryDatabases = phdBases

	// starting up a pruner
	logging.Info("Starting up the pricelist-histories file pruner")
	prunerStop := make(state.WorkerStopChan)
	onPrunerStop := phdBases.StartPruner(prunerStop)

	// opening all listeners
	logging.Info("Opening all listeners")
	sta.Listeners = state.NewListeners(state.SubjectListeners{
		subjects.PricelistsIntake: sta.ListenForPricelistsIntake,
		subjects.PriceListHistory: sta.ListenForPriceListHistory,
	})
	if err := sta.Listeners.Listen(); err != nil {
		return err
	}

	// catching SIGINT
	logging.Info("Waiting for SIGINT")
	sigIn := make(chan os.Signal, 1)
	signal.Notify(sigIn, os.Interrupt)
	<-sigIn

	logging.Info("Caught SIGINT, exiting")

	// stopping listeners
	sta.Listeners.Stop()

	logging.Info("Stopping pruner")
	prunerStop <- struct{}{}

	logging.Info("Waiting for pruner to stop")
	<-onPrunerStop

	logging.Info("Exiting")
	return nil
}
