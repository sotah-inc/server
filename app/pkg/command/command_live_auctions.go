package command

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/store"

	"github.com/sotah-inc/server/app/internal"

	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
	"github.com/sotah-inc/server/app/pkg/util"
)

func liveAuctionsCacheDirs(c internal.Config, regions internal.RegionList, stas internal.Statuses) ([]string, error) {
	// ensuring cache-dirs exist
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

func liveAuctions(c internal.Config, m messenger.Messenger, s store.Store) error {
	logging.Info("Starting live-auctions")

	// establishing a state
	res := internal.NewResolver(c, m, s)
	sta := state.NewState(res)

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
	cacheDirs, err := liveAuctionsCacheDirs(c, sta.Regions, sta.Statuses)
	if err := util.EnsureDirsExist(cacheDirs); err != nil {
		return err
	}

	// loading up live-auction databases
	ladBases, err := database.NewLiveAuctionsDatabases(c, sta.Regions, sta.Statuses)
	if err != nil {
		return err
	}
	sta.IO.databases.LiveAuctionsDatabases = ladBases

	// opening all listeners
	sta.Listeners = state.NewListeners(state.SubjectListeners{
		subjects.Auctions:           sta.ListenForAuctions,
		subjects.LiveAuctionsIntake: sta.ListenForLiveAuctionsIntake,
		subjects.PriceList:          sta.ListenForPriceList,
		subjects.Owners:             sta.ListenForOwners,
		subjects.OwnersQueryByItems: sta.ListenForOwnersQueryByItems,
		subjects.OwnersQuery:        sta.ListenForOwnersQuery,
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

	logging.Info("Exiting")
	return nil
}
