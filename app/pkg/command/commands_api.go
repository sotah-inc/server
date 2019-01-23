package command

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/sotah-inc/server/app/pkg/database"

	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/store"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/internal"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/util"
	"github.com/twinj/uuid"
)

func apiCacheDirs(c internal.Config, regions internal.RegionList) ([]string, error) {
	databaseDir, err := c.DatabaseDir()
	if err != nil {
		return nil, err
	}

	cacheDirs := []string{databaseDir, fmt.Sprintf("%s/items", c.CacheDir)}
	if !c.UseGCloud {
		cacheDirs = append(cacheDirs, fmt.Sprintf("%s/auctions", c.CacheDir))
		for _, reg := range regions {
			cacheDirs = append(cacheDirs, fmt.Sprintf("%s/auctions/%s", c.CacheDir, reg.Name))
		}
	}

	return cacheDirs, nil
}

func api(c internal.Config, m messenger.Messenger, s store.Store) error {
	logging.Info("Starting api")

	// establishing a state
	res := internal.NewResolver(c, m, s)
	sta := state.NewState(m, res)

	// creating a uuid4 api-session secret and run-id of state
	sta.RunID = uuid.NewV4()
	sta.SessionSecret = uuid.NewV4()

	// ensuring cache-dirs exist
	cacheDirs, err := apiCacheDirs(c, sta.Regions)
	if err != nil {
		return err
	}

	if err := util.EnsureDirsExist(cacheDirs); err != nil {
		return err
	}

	// loading up items database
	idBase, err := database.NewItemsDatabase(c)
	if err != nil {
		return err
	}
	sta.ItemsDatabase = idBase

	// refreshing the access-token for the resolver blizz client
	nextClient, err := res.BlizzardClient.RefreshFromHTTP(blizzard.OAuthTokenEndpoint)
	if err != nil {
		logging.WithField("error", err.Error()).Error("Failed to refresh blizzard client")

		return err
	}
	res.BlizzardClient = nextClient
	sta.Resolver = res

	// filling state with region statuses
	for _, reg := range sta.Regions {
		regionStatus, err := reg.GetStatus(res)
		if err != nil {
			logging.WithFields(logrus.Fields{
				"error":  err.Error(),
				"region": reg.Name,
			}).Error("Failed to fetch status")

			return err
		}

		regionStatus.Realms = c.FilterInRealms(reg, regionStatus.Realms)
		sta.Statuses[reg.Name] = regionStatus
	}

	// gathering item-classes
	primaryRegion, err := c.Regions.GetPrimaryRegion()
	if err != nil {
		return err
	}

	uri, err := res.AppendAccessToken(res.GetItemClassesURL(primaryRegion.Hostname))
	if err != nil {
		return err
	}

	iClasses, _, err := blizzard.NewItemClassesFromHTTP(uri)
	if err != nil {
		return err
	}
	sta.ItemClasses = iClasses

	// gathering profession icons into storage
	if c.UseGCloud {
		iconNames := make([]string, len(c.Professions))
		for i, prof := range c.Professions {
			iconNames[i] = prof.Icon
		}

		syncedIcons, err := s.SyncItemIcons(iconNames, res)
		if err != nil {
			return err
		}
		for job := range syncedIcons {
			if job.Err != nil {
				return job.Err
			}

			for i, prof := range c.Professions {
				if prof.Icon != job.IconName {
					continue
				}

				c.Professions[i].IconURL = job.IconURL
			}
		}
	} else {
		for i, prof := range c.Professions {
			c.Professions[i].IconURL = internal.DefaultGetItemIconURL(prof.Icon)
		}
	}

	// opening all listeners
	sta.Listeners = state.NewListeners(state.SubjectListeners{
		subjects.GenericTestErrors: sta.ListenForGenericTestErrors,
		subjects.Status:            sta.ListenForStatus,
		subjects.Regions:           sta.ListenForRegions,
		subjects.ItemsQuery:        sta.ListenForItemsQuery,
		subjects.ItemClasses:       sta.ListenForItemClasses,
		subjects.Items:             sta.ListenForItems,
		subjects.Boot:              sta.ListenForBoot,
		subjects.SessionSecret:     sta.ListenForSessionSecret,
		subjects.RuntimeInfo:       sta.ListenForRuntimeInfo,
	})
	if err := sta.Listeners.Listen(); err != nil {
		return err
	}

	// starting up a collector
	collectorStop := make(state.WorkerStopChan)
	onCollectorStop := sta.StartCollector(collectorStop, res)

	// catching SIGINT
	logging.Info("Waiting for SIGINT")
	sigIn := make(chan os.Signal, 1)
	signal.Notify(sigIn, os.Interrupt)
	<-sigIn

	logging.Info("Caught SIGINT, exiting")

	// stopping listeners
	sta.Listeners.Stop()

	logging.Info("Stopping collector")
	collectorStop <- struct{}{}

	logging.Info("Waiting for collector to stop")
	<-onCollectorStop

	logging.Info("Exiting")
	return nil
}
