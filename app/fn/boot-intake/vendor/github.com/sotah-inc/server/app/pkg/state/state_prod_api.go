package state

import (
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
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

type ProdApiStateConfig struct {
	SotahConfig sotah.Config

	GCloudProjectID string

	MessengerHost string
	MessengerPort int

	BlizzardClientId     string
	BlizzardClientSecret string

	ItemsDatabaseDir string
}

func NewProdApiState(config ProdApiStateConfig) (ProdApiState, error) {
	// establishing an initial state
	apiState := ProdApiState{
		State: NewState(uuid.NewV4(), config.SotahConfig.UseGCloud),
	}
	apiState.SessionSecret = uuid.NewV4()

	// setting api-state from config, including filtering in regions based on config whitelist
	apiState.Regions = config.SotahConfig.FilterInRegions(config.SotahConfig.Regions)
	apiState.Expansions = config.SotahConfig.Expansions
	apiState.Professions = config.SotahConfig.Professions
	apiState.ItemBlacklist = config.SotahConfig.ItemBlacklist
	apiState.BlizzardClientId = config.BlizzardClientId
	apiState.BlizzardClientSecret = config.BlizzardClientSecret

	// establishing a store
	stor, err := store.NewClient(config.GCloudProjectID)
	if err != nil {
		return ProdApiState{}, err
	}
	apiState.IO.StoreClient = stor

	// establishing a bus
	logging.Info("Connecting bus-client")
	busClient, err := bus.NewClient(config.GCloudProjectID, "prod-api")
	apiState.IO.BusClient = busClient

	// connecting to the messenger host
	mess, err := messenger.NewMessenger(config.MessengerHost, config.MessengerPort)
	if err != nil {
		return ProdApiState{}, err
	}
	apiState.IO.Messenger = mess

	// initializing a reporter
	apiState.IO.Reporter = metric.NewReporter(mess)

	// connecting a new blizzard client
	blizzardClient, err := blizzard.NewClient(config.BlizzardClientId, config.BlizzardClientSecret)
	if err != nil {
		return ProdApiState{}, err
	}
	apiState.IO.Resolver = resolver.NewResolver(blizzardClient, apiState.IO.Reporter)

	// filling state with region statuses
	for job := range apiState.IO.Resolver.GetStatuses(apiState.Regions) {
		if job.Err != nil {
			return ProdApiState{}, job.Err
		}

		region := job.Region
		status := job.Status

		status.Realms = config.SotahConfig.FilterInRealms(region, status.Realms)
		apiState.Statuses[region.Name] = status
	}

	// filling state with item-classes
	primaryRegion, err := apiState.Regions.GetPrimaryRegion()
	if err != nil {
		logging.WithFields(logrus.Fields{
			"error":   err.Error(),
			"regions": apiState.Regions,
		}).Error("Failed to retrieve primary region")

		return ProdApiState{}, err
	}
	uri, err := apiState.IO.Resolver.AppendAccessToken(apiState.IO.Resolver.GetItemClassesURL(primaryRegion.Hostname))
	if err != nil {
		return ProdApiState{}, err
	}
	itemClasses, _, err := blizzard.NewItemClassesFromHTTP(uri)
	if err != nil {
		return ProdApiState{}, err
	}
	apiState.ItemClasses = itemClasses

	// gathering profession icons
	for i, prof := range apiState.Professions {
		itemIconUrl, err := func() (string, error) {
			exists, err := apiState.IO.StoreClient.ItemIconExists(prof.Icon)
			if err != nil {
				return "", err
			}

			if exists {
				obj, err := apiState.IO.StoreClient.GetItemIconObject(prof.Icon)
				if err != nil {
					return "", err
				}

				return apiState.IO.StoreClient.GetStoreItemIconURLFunc(obj)
			}

			body, err := util.Download(blizzard.DefaultGetItemIconURL(prof.Icon))
			if err != nil {
				return "", err
			}

			return apiState.IO.StoreClient.WriteItemIcon(prof.Icon, body)
		}()
		if err != nil {
			return ProdApiState{}, err
		}

		apiState.Professions[i].IconURL = itemIconUrl
	}

	// establishing bus-listeners
	apiState.BusListeners = NewBusListeners(SubjectBusListeners{
		subjects.Boot:   apiState.ListenForBoot,
		subjects.Status: apiState.ListenForStatus,
	})

	return apiState, nil
}

type ProdApiState struct {
	State

	SessionSecret uuid.UUID
	ItemClasses   blizzard.ItemClasses
	Expansions    []sotah.Expansion
	Professions   []sotah.Profession
	ItemBlacklist ItemBlacklist

	BlizzardClientId     string
	BlizzardClientSecret string
}
