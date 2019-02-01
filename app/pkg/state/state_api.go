package state

import (
	"fmt"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/diskstore"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
	"github.com/sotah-inc/server/app/pkg/resolver"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/sotah-inc/server/app/pkg/util"
	"github.com/twinj/uuid"
)

type ItemBlacklistMap map[blizzard.ItemID]struct{}

type APIStateConfig struct {
	sotahConfig sotah.Config

	UseGCloud       bool
	GCloudProjectID string

	MessengerHost string
	MessengerPort int

	DiskStoreCacheDir string

	BlizzardClientId     string
	BlizzardClientSecret string

	ItemsDatabaseDir string

	ItemBlacklist ItemBlacklistMap
}

func NewAPIState(config APIStateConfig) (APIState, error) {
	// establishing an initial state
	apiState := APIState{
		State: NewState(uuid.NewV4(), config.UseGCloud),
	}
	apiState.SessionSecret = uuid.NewV4()

	// setting api-state from config, including filtering in regions based on config whitelist
	apiState.Regions = config.sotahConfig.FilterInRegions(config.sotahConfig.Regions)
	apiState.Expansions = config.sotahConfig.Expansions
	apiState.Professions = config.sotahConfig.Professions
	apiState.ItemBlacklist = config.ItemBlacklist

	// establishing a store (gcloud store or disk store)
	if config.UseGCloud {
		stor, err := store.NewStore(config.GCloudProjectID)
		if err != nil {
			return APIState{}, err
		}

		apiState.IO.Store = stor
	} else {
		cacheDirs := []string{
			config.DiskStoreCacheDir,
			fmt.Sprintf("%s/items", config.DiskStoreCacheDir),
			fmt.Sprintf("%s/auctions", config.DiskStoreCacheDir),
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

	// connecting a new blizzard client
	blizzardClient, err := blizzard.NewClient(config.BlizzardClientId, config.BlizzardClientSecret)
	if err != nil {
		return APIState{}, err
	}
	apiState.IO.Resolver = resolver.NewResolver(blizzardClient)

	// filling state with region statuses
	for _, reg := range apiState.Regions {
		status, _, err := blizzard.NewStatusFromHTTP(blizzard.DefaultGetStatusURL(reg.Hostname))
		if err != nil {
			return APIState{}, err
		}

		sotahStatus := sotah.NewStatus(reg, status)
		sotahStatus.Realms = config.sotahConfig.FilterInRealms(reg, sotah.NewRealms(reg, status.Realms))
		apiState.Statuses[reg.Name] = sotahStatus
	}

	// filling state with item-classes
	primaryRegion, err := apiState.Regions.GetPrimaryRegion()
	if err != nil {
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
	if apiState.UseGCloud {
		for i, prof := range apiState.Professions {
			itemIconUrl, err := func() (string, error) {
				exists, err := apiState.IO.Store.ItemIconExists(prof.Icon)
				if err != nil {
					return "", err
				}

				if exists {
					obj, err := apiState.IO.Store.GetItemIconObject(prof.Icon)
					if err != nil {
						return "", err
					}

					return apiState.IO.Store.GetStoreItemIconURLFunc(obj)
				}

				body, err := util.Download(blizzard.DefaultGetItemIconURL(prof.Icon))
				if err != nil {
					return "", err
				}

				return apiState.IO.Store.WriteItemIcon(prof.Icon, body)
			}()
			if err != nil {
				return APIState{}, err
			}

			apiState.Professions[i].Icon = itemIconUrl
		}
	} else {
		for i, prof := range apiState.Professions {
			apiState.Professions[i].Icon = blizzard.DefaultGetItemIconURL(prof.Icon)
		}
	}

	// establishing listeners
	apiState.Listeners = NewListeners(SubjectListeners{
		subjects.Boot:          apiState.ListenForBoot,
		subjects.SessionSecret: apiState.ListenForSessionSecret,
		subjects.Status:        apiState.ListenForStatus,
		subjects.Items:         apiState.ListenForItems,
		subjects.ItemsQuery:    apiState.ListenForItemsQuery,
	})

	return apiState, nil
}

type APIState struct {
	State

	SessionSecret uuid.UUID
	ItemClasses   blizzard.ItemClasses
	Expansions    []sotah.Expansion
	Professions   []sotah.Profession
	ItemBlacklist ItemBlacklistMap
}