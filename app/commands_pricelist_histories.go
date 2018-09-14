package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/ihsw/sotah-server/app/logging"
	"github.com/ihsw/sotah-server/app/subjects"
	"github.com/ihsw/sotah-server/app/util"
)

func pricelistHistories(c config, m messenger, s store) error {
	logging.Info("Starting pricelist-histories")

	// establishing a state
	res := newResolver(c, m, s)
	sta := newState(m, res)

	// gathering region-status from the root service
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
	for _, reg := range regions {
		if c.Whitelist[reg.Name] != nil && len(*c.Whitelist[reg.Name]) == 0 {
			logging.WithField("region", reg.Name).Debug("Filtering out region from initialization")

			continue
		}

		regionStatus, err := newStatusFromMessenger(*reg, m)
		if err != nil {
			logging.WithField("region", reg.Name).Info("Could not fetch status for region")

			return err
		}
		sta.statuses[reg.Name] = regionStatus
	}

	// ensuring cache-dirs exist
	databaseDir, err := c.databaseDir()
	if err != nil {
		return err
	}
	cacheDirs := []string{databaseDir}
	for _, reg := range sta.regions {
		regionDatabaseDir := reg.databaseDir(databaseDir)
		cacheDirs = append(cacheDirs, regionDatabaseDir)

		for _, rea := range sta.statuses[reg.Name].Realms {
			cacheDirs = append(cacheDirs, rea.databaseDir(regionDatabaseDir))
		}
	}
	if err := util.EnsureDirsExist(cacheDirs); err != nil {
		return err
	}

	// loading up databases
	sta.databases = newDatabases(sta)

	// opening all listeners
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

	logging.Info("Exiting")
	return nil
}
