package command

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/logging"
	"github.com/sotah-inc/server/app/subjects"
	"github.com/sotah-inc/server/app/util"
)

func pricelistHistoriesCacheDirs(c config, regions regionList, stas statuses) ([]string, error) {
	databaseDir, err := c.databaseDir()
	if err != nil {
		return nil, err
	}

	cacheDirs := []string{databaseDir}
	for _, reg := range regions {
		regionDatabaseDir := reg.databaseDir(databaseDir)
		cacheDirs = append(cacheDirs, regionDatabaseDir)

		for _, rea := range stas[reg.Name].Realms {
			cacheDirs = append(cacheDirs, rea.databaseDir(regionDatabaseDir))
		}
	}

	return cacheDirs, nil
}

func pricelistHistories(c config, m messenger, s store) error {
	logging.Info("Starting pricelist-histories")

	// establishing a state
	res := newResolver(c, m, s)
	sta := newState(m, res)

	// gathering region-status from the root service
	logging.Info("Gathering regions")
	regions, err := func() (regionList, error) {
		out := regionList{}
		attempts := 0
		for {
			var err error
			out, err = newRegionsFromMessenger(m)
			if err == nil {
				break
			} else {
				logging.Info("Could not fetch regions, retrying in 250ms")

				attempts++
				time.Sleep(250 * time.Millisecond)
			}

			if attempts >= 20 {
				return regionList{}, fmt.Errorf("Failed to fetch regions after %d attempts", attempts)
			}
		}

		return out, nil
	}()
	if err != nil {
		logging.WithField("error", err.Error()).Error("Failed to fetch regions")

		return err
	}

	sta.regions = c.filterInRegions(regions)

	// filling state with statuses
	logging.Info("Gathering statuses")
	for _, reg := range sta.regions {
		regionStatus, err := newStatusFromMessenger(reg, m)
		if err != nil {
			logging.WithField("region", reg.Name).Info("Could not fetch status for region")

			return err
		}
		regionStatus.Realms = c.filterInRealms(reg, regionStatus.Realms)
		sta.statuses[reg.Name] = regionStatus
	}

	// ensuring cache-dirs exist
	logging.Info("Ensuring cache-dirs exist")
	cacheDirs, err := pricelistHistoriesCacheDirs(c, sta.regions, sta.statuses)
	if err != nil {
		return err
	}

	if err := util.EnsureDirsExist(cacheDirs); err != nil {
		return err
	}

	// pruning old data
	databaseDir, err := c.databaseDir()
	if err != nil {
		return err
	}

	earliestTime := databaseRetentionLimit()
	for _, reg := range sta.regions {
		regionDatabaseDir := reg.databaseDir(databaseDir)

		for _, rea := range sta.statuses[reg.Name].Realms {
			realmDatabaseDir := rea.databaseDir(regionDatabaseDir)
			dbPaths, err := databasePaths(realmDatabaseDir)
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error": err.Error(),
					"dir":   realmDatabaseDir,
				}).Error("Failed to resolve database paths")

				return err
			}
			for _, dbPathPair := range dbPaths {
				if dbPathPair.targetTime.After(earliestTime) {
					continue
				}

				logging.WithFields(logrus.Fields{
					"pathname": dbPathPair.fullPath,
				}).Debug("Pruning old pricelist-history database file")

				if err := os.Remove(dbPathPair.fullPath); err != nil {
					logging.WithFields(logrus.Fields{
						"error":    err.Error(),
						"dir":      realmDatabaseDir,
						"pathname": dbPathPair.fullPath,
					}).Error("Failed to remove database file")

					return err
				}
			}
		}
	}

	// loading up databases
	logging.Info("Loading up databases")
	phdBases, err := newPricelistHistoryDatabases(c, sta.regions, sta.statuses)
	if err != nil {
		logging.WithField("error", err.Error()).Error("Failed to load databases")

		return err
	}
	sta.pricelistHistoryDatabases = phdBases

	// starting up a pruner
	logging.Info("Starting up the pricelist-histories file pruner")
	prunerStop := make(workerStopChan)
	onPrunerStop := phdBases.startPruner(prunerStop)

	// opening all listeners
	logging.Info("Opening all listeners")
	sta.listeners = newListeners(subjectListeners{
		subjects.PricelistsIntake: sta.listenForPricelistsIntake,
		subjects.PriceListHistory: sta.listenForPriceListHistory,
	})
	if err := sta.listeners.listen(); err != nil {
		return err
	}

	// catching SIGINT
	logging.Info("Waiting for SIGINT")
	sigIn := make(chan os.Signal, 1)
	signal.Notify(sigIn, os.Interrupt)
	<-sigIn

	logging.Info("Caught SIGINT, exiting")

	// stopping listeners
	sta.listeners.stop()

	logging.Info("Stopping pruner")
	prunerStop <- struct{}{}

	logging.Info("Waiting for pruner to stop")
	<-onPrunerStop

	logging.Info("Exiting")
	return nil
}
