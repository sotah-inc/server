package state

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/diskstore"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/resolver"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/sotah-inc/server/app/pkg/util"
	"github.com/twinj/uuid"
)

type APIStateConfig struct {
	SotahConfig sotah.Config

	GCloudProjectID string

	MessengerHost string
	MessengerPort int

	DiskStoreCacheDir string

	BlizzardClientId     string
	BlizzardClientSecret string

	ItemsDatabaseDir string
}

func NewAPIState(config APIStateConfig) (APIState, error) {
	// establishing an initial state
	apiState := APIState{
		State: NewState(uuid.NewV4(), config.SotahConfig.UseGCloud),
	}
	apiState.SessionSecret = uuid.NewV4()

	// setting api-state from config, including filtering in regions based on config whitelist
	apiState.Regions = config.SotahConfig.FilterInRegions(config.SotahConfig.Regions)
	apiState.Expansions = config.SotahConfig.Expansions
	apiState.Professions = config.SotahConfig.Professions
	apiState.ItemBlacklist = config.SotahConfig.ItemBlacklist

	// establishing a store (gcloud store or disk store)
	if config.SotahConfig.UseGCloud {
		stor, err := store.NewClient(config.GCloudProjectID)
		if err != nil {
			return APIState{}, err
		}

		apiState.IO.StoreClient = stor

		// establishing a bus
		logging.Info("Connecting bus-client")
		busClient, err := bus.NewClient(config.GCloudProjectID, "api")
		if err != nil {
			return APIState{}, err
		}
		apiState.IO.BusClient = busClient
	} else {
		cacheDirs := []string{
			config.DiskStoreCacheDir,
			fmt.Sprintf("%s/items", config.DiskStoreCacheDir),
			fmt.Sprintf("%s/auctions", config.DiskStoreCacheDir),
			fmt.Sprintf("%s/databases", config.DiskStoreCacheDir),
		}
		for _, reg := range apiState.Regions {
			cacheDirs = append(cacheDirs, fmt.Sprintf("%s/auctions/%s", config.DiskStoreCacheDir, reg.Name))
		}
		if err := util.EnsureDirsExist(cacheDirs); err != nil {
			return APIState{}, err
		}

		apiState.IO.DiskStore = diskstore.NewDiskStore(config.DiskStoreCacheDir)
	}

	// connecting to the messenger host
	mess, err := messenger.NewMessenger(config.MessengerHost, config.MessengerPort)
	if err != nil {
		return APIState{}, err
	}
	apiState.IO.Messenger = mess

	// initializing a reporter
	apiState.IO.Reporter = metric.NewReporter(mess)

	// connecting a new blizzard client
	blizzardClient, err := blizzard.NewClient(config.BlizzardClientId, config.BlizzardClientSecret)
	if err != nil {
		return APIState{}, err
	}
	apiState.IO.Resolver = resolver.NewResolver(blizzardClient, apiState.IO.Reporter)

	// filling state with region statuses
	for job := range apiState.IO.Resolver.GetStatuses(apiState.Regions) {
		if job.Err != nil {
			return APIState{}, job.Err
		}

		job.Status.Realms = config.SotahConfig.FilterInRealms(job.Region, job.Status.Realms)
		apiState.Statuses[job.Region.Name] = job.Status
	}

	// filling state with item-classes
	primaryRegion, err := apiState.Regions.GetPrimaryRegion()
	if err != nil {
		logging.WithFields(logrus.Fields{
			"error":   err.Error(),
			"regions": apiState.Regions,
		}).Error("Failed to retrieve primary region")

		return APIState{}, err
	}
	uri, err := apiState.IO.Resolver.AppendAccessToken(apiState.IO.Resolver.GetItemClassesURL(primaryRegion.Hostname))
	if err != nil {
		return APIState{}, err
	}
	itemClasses, _, err := blizzard.NewItemClassesFromHTTP(uri)
	if err != nil {
		return APIState{}, err
	}
	apiState.ItemClasses = itemClasses

	// loading the items database
	itemsDatabase, err := database.NewItemsDatabase(config.ItemsDatabaseDir)
	if err != nil {
		return APIState{}, err
	}
	apiState.IO.Databases.ItemsDatabase = itemsDatabase

	// gathering profession icons
	for i, prof := range apiState.Professions {
		apiState.Professions[i].IconURL = blizzard.DefaultGetItemIconURL(prof.Icon)
	}

	// establishing listeners
	apiState.Listeners = NewListeners(SubjectListeners{
		subjects.Boot:                   apiState.ListenForBoot,
		subjects.SessionSecret:          apiState.ListenForSessionSecret,
		subjects.Status:                 apiState.ListenForStatus,
		subjects.Items:                  apiState.ListenForItems,
		subjects.ItemsQuery:             apiState.ListenForItemsQuery,
		subjects.RealmModificationDates: apiState.ListenForRealmModificationDates,
	})

	apiState.RegionRealmModificationDates = sotah.RegionRealmModificationDates{}

	return apiState, nil
}

type APIState struct {
	State

	SessionSecret uuid.UUID
	ItemClasses   blizzard.ItemClasses
	Expansions    []sotah.Expansion
	Professions   []sotah.Profession
	ItemBlacklist ItemBlacklist

	RegionRealmModificationDates sotah.RegionRealmModificationDates
}

type ItemBlacklist []blizzard.ItemID

func (ib ItemBlacklist) IsPresent(itemId blizzard.ItemID) bool {
	for _, blacklistItemId := range ib {
		if blacklistItemId == itemId {
			return true
		}
	}

	return false
}
