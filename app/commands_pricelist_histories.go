package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ihsw/sotah-server/app/logging"
	"github.com/ihsw/sotah-server/app/subjects"
	"github.com/ihsw/sotah-server/app/util"
	"github.com/sirupsen/logrus"
)

func pricelistHistories(c config, m messenger, s store) error {
	logging.Info("Starting pricelist-histories")

	// establishing a state
	res := newResolver(c, m, s)
	sta := newState(m, res)

	// gathering region-status from the root service
	logging.Info("Gathering regions")
	regions := []*region{}
	attempts := 0
	for {
		var err error
		regions, err = newRegionsFromMessenger(m)
		if err == nil {
			break
		} else {
			logging.Info("Could not fetch regions, retrying in 250ms")

			attempts++
			time.Sleep(250 * time.Millisecond)
		}

		if attempts >= 20 {
			return fmt.Errorf("Failed to fetch regions after %d attempts", attempts)
		}
	}

	for i, reg := range regions {
		sta.regions[i] = *reg
	}

	// filling state with statuses
	logging.Info("Gathering statuses")
	for _, reg := range c.filterInRegions(sta.regions) {
		regionStatus, err := newStatusFromMessenger(reg, m)
		if err != nil {
			logging.WithField("region", reg.Name).Info("Could not fetch status for region")

			return err
		}
		sta.statuses[reg.Name] = regionStatus
	}

	// ensuring cache-dirs exist
	logging.Info("Ensuring cache-dirs exist")
	databaseDir, err := c.databaseDir()
	if err != nil {
		return err
	}
	cacheDirs := []string{databaseDir}
	for _, reg := range c.filterInRegions(sta.regions) {
		regionDatabaseDir := reg.databaseDir(databaseDir)
		cacheDirs = append(cacheDirs, regionDatabaseDir)

		for _, rea := range c.filterInRealms(reg, sta.statuses[reg.Name].Realms) {
			cacheDirs = append(cacheDirs, rea.databaseDir(regionDatabaseDir))
		}
	}
	if err := util.EnsureDirsExist(cacheDirs); err != nil {
		return err
	}

	// pruning old data
	logging.Info("Pruning old data")
	earliestTime := databaseRetentionLimit()
	for _, reg := range c.filterInRegions(sta.regions) {
		regionDatabaseDir := reg.databaseDir(databaseDir)

		for _, rea := range c.filterInRealms(reg, sta.statuses[reg.Name].Realms) {
			realmDatabaseDir := rea.databaseDir(regionDatabaseDir)
			databaseFilepaths, err := ioutil.ReadDir(realmDatabaseDir)
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error": err.Error(),
					"dir":   realmDatabaseDir,
				}).Error("Failed to read database dir")

				return err
			}

			for _, fPath := range databaseFilepaths {
				if fPath.Name() == "live-auctions.db" {
					continue
				}

				parts := strings.Split(fPath.Name(), ".")
				targetTimeUnix, err := strconv.Atoi(parts[0])
				if err != nil {
					logging.WithFields(logrus.Fields{
						"error":    err.Error(),
						"dir":      realmDatabaseDir,
						"pathname": fPath.Name(),
					}).Error("Failed to parse database filepath")

					return err
				}

				targetTime := time.Unix(int64(targetTimeUnix), 0)
				if targetTime.After(earliestTime) {
					continue
				}

				fullPath, err := filepath.Abs(fmt.Sprintf("%s/%s", realmDatabaseDir, fPath.Name()))
				if err != nil {
					logging.WithFields(logrus.Fields{
						"error":    err.Error(),
						"dir":      realmDatabaseDir,
						"pathname": fPath.Name(),
					}).Error("Failed to resolve full path of database file")

					return err
				}

				if err := os.Remove(fullPath); err != nil {
					logging.WithFields(logrus.Fields{
						"error":    err.Error(),
						"dir":      realmDatabaseDir,
						"pathname": fPath.Name(),
					}).Error("Failed to remove database file")

					return err
				}
			}
		}
	}

	// loading up databases
	logging.Info("Loading up databases")
	dBases, err := newDatabases(c, sta.regions, sta.statuses)
	if err != nil {
		return err
	}
	sta.databases = dBases

	// starting up a pruner
	logging.Info("Starting up the pricelist-histories file pruner")
	prunerStop := make(workerStopChan)
	onPrunerStop := dBases.startPruner(prunerStop)

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
