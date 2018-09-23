package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/ihsw/sotah-server/app/blizzard"

	"github.com/ihsw/sotah-server/app/logging"
	"github.com/ihsw/sotah-server/app/util"
)

func reformHistory(c config, m messenger, s store) error {
	logging.Info("Starting reform-history")

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

	// validating database-dirs exist
	logging.Info("Validating database-dirs exist")
	databaseDir, err := c.databaseDir()
	if err != nil {
		return err
	}
	exists, err := util.StatExists(databaseDir)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("Database dir does not exist")
	}
	for _, reg := range c.filterInRegions(sta.regions) {
		regionDatabaseDir := reg.databaseDir(databaseDir)
		exists, err := util.StatExists(regionDatabaseDir)
		if err != nil {
			return err
		}
		if !exists {
			return errors.New("Region dir does not exist")
		}

		for _, rea := range c.filterInRealms(reg, sta.statuses[reg.Name].Realms) {
			exists, err := util.StatExists(rea.databaseDir(regionDatabaseDir))
			if err != nil {
				return err
			}
			if !exists {
				return errors.New("Realm dir does not exist")
			}
		}
	}

	// loading up databases
	logging.Info("Loading databases")
	dBases, err := newDatabases(c, sta.regions, sta.statuses)
	if err != nil {
		return err
	}

	// going over the databases and gathering their region/realm/target-date/size mapping
	logging.Info("Gathering all database file sizes")
	currentSizes := map[regionName]map[blizzard.RealmSlug]map[int64]int64{}
	for _, reg := range c.filterInRegions(sta.regions) {
		currentSizes[reg.Name] = map[blizzard.RealmSlug]map[int64]int64{}

		for _, rea := range c.filterInRealms(reg, sta.statuses[reg.Name].Realms) {
			currentSizes[reg.Name][rea.Slug] = map[int64]int64{}

			for targetUnixTime, dBase := range dBases[reg.Name][rea.Slug] {
				stat, err := os.Stat(dBase.db.Path())
				if err != nil {
					return err
				}

				currentSizes[reg.Name][rea.Slug][targetUnixTime] = stat.Size()
			}
		}
	}

	return nil
}
