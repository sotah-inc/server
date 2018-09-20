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

func liveAuctions(c config, m messenger, s store) error {
	logging.Info("Starting live-auctions")

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
			logging.WithField("attempts", attempts).Info("Could not fetch regions, retrying in 250ms")

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
		regionStatus.Realms = c.filterInRealms(reg, regionStatus.Realms)
		sta.statuses[reg.Name] = regionStatus
	}

	// ensuring cache-dirs exist
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

	// loading up live-auction databases
	ladBases, err := newLiveAuctionsDatabases(c, sta.regions, sta.statuses)
	if err != nil {
		return err
	}
	sta.liveAuctionsDatabases = ladBases

	// loading up auctions
	for _, reg := range sta.regions {
		loadedAuctions := sta.statuses[reg.Name].Realms.loadAuctions(&c, s)
		for job := range loadedAuctions {
			if job.err != nil {
				return job.err
			}

			// pushing the auctions onto the state
			sta.liveAuctionsDatabases[reg.Name][job.realm.Slug].persistMiniauctions(newMiniAuctionListFromBlizzardAuctions(job.auctions.Auctions))

			// setting the realm last-modified
			for i, statusRealm := range sta.statuses[reg.Name].Realms {
				if statusRealm.Slug != job.realm.Slug {
					continue
				}

				sta.statuses[reg.Name].Realms[i].LastModified = job.lastModified.Unix()

				break
			}
		}
	}

	// opening all listeners
	sta.listeners = newListeners(subjectListeners{
		subjects.Auctions:           sta.listenForAuctions,
		subjects.AuctionsIntake:     sta.listenForAuctionsIntake,
		subjects.PriceList:          sta.listenForPriceList,
		subjects.Owners:             sta.listenForOwners,
		subjects.OwnersQueryByItems: sta.listenForOwnersQueryByItems,
		subjects.OwnersQuery:        sta.listenForOwnersQuery,
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
