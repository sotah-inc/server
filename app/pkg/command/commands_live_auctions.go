package command

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/sotah-inc/server/app/logging"
	"github.com/sotah-inc/server/app/subjects"
	"github.com/sotah-inc/server/app/util"
)

func liveAuctionsCacheDirs(c config, regions regionList, stas statuses) ([]string, error) {
	// ensuring cache-dirs exist
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

func liveAuctions(c config, m messenger, s store) error {
	logging.Info("Starting live-auctions")

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
	cacheDirs, err := liveAuctionsCacheDirs(c, sta.regions, sta.statuses)
	if err := util.EnsureDirsExist(cacheDirs); err != nil {
		return err
	}

	// loading up live-auction databases
	ladBases, err := newLiveAuctionsDatabases(c, sta.regions, sta.statuses)
	if err != nil {
		return err
	}
	sta.liveAuctionsDatabases = ladBases

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
