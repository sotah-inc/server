package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/ihsw/sotah-server/app/logging"
	"github.com/ihsw/sotah-server/app/util"
)

func reformHistory(c config, m messenger, s store) error {
	logging.Info("Starting reform-history")

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
	for _, reg := range c.filterInRegions(sta.regions) {
		regionStatus, err := newStatusFromMessenger(reg, m)
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

	return nil
}
