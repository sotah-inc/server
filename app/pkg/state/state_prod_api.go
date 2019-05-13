package state

import (
	"fmt"
	"log"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/hell"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/resolver"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/sotah/gameversions"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/sotah-inc/server/app/pkg/store/regions"
	"github.com/sotah-inc/server/app/pkg/util"
	"github.com/twinj/uuid"
)

type ProdApiStateConfig struct {
	SotahConfig sotah.Config

	GCloudProjectID string

	MessengerHost string
	MessengerPort int
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

	// establishing a store
	stor, err := store.NewClient(config.GCloudProjectID)
	if err != nil {
		return ProdApiState{}, err
	}
	apiState.IO.StoreClient = stor

	bootBase := store.NewBootBase(apiState.IO.StoreClient, regions.USCentral1)

	var bootBucket *storage.BucketHandle
	bootBucket, err = bootBase.GetFirmBucket()
	if err != nil {
		return ProdApiState{}, err
	}
	blizzardCredentials, err := bootBase.GetBlizzardCredentials(bootBucket)
	if err != nil {
		return ProdApiState{}, err
	}

	apiState.RealmsBase = store.NewRealmsBase(apiState.IO.StoreClient, regions.USCentral1, gameversions.Retail)
	apiState.RealmsBucket, err = apiState.RealmsBase.GetFirmBucket()
	if err != nil {
		return ProdApiState{}, err
	}

	// establishing a bus
	logging.Info("Connecting bus-client")
	busClient, err := bus.NewClient(config.GCloudProjectID, "prod-api")
	if err != nil {
		return ProdApiState{}, err
	}
	apiState.IO.BusClient = busClient

	// connecting to hell
	apiState.IO.HellClient, err = hell.NewClient(config.GCloudProjectID)
	if err != nil {
		log.Fatalf("Failed to connect to firebase: %s", err.Error())

		return ProdApiState{}, err
	}

	// connecting to the messenger host
	mess, err := messenger.NewMessenger(config.MessengerHost, config.MessengerPort)
	if err != nil {
		return ProdApiState{}, err
	}
	apiState.IO.Messenger = mess

	// initializing a reporter
	apiState.IO.Reporter = metric.NewReporter(mess)

	// connecting a new blizzard client
	blizzardClient, err := blizzard.NewClient(blizzardCredentials.ClientId, blizzardCredentials.ClientSecret)
	if err != nil {
		return ProdApiState{}, err
	}
	apiState.IO.Resolver = resolver.NewResolver(blizzardClient, apiState.IO.Reporter)

	// filling state with region statuses
	for _, region := range apiState.Regions {
		realms, err := apiState.RealmsBase.GetAllRealms(region.Name, apiState.RealmsBucket)
		if err != nil {
			return ProdApiState{}, err
		}

		status := apiState.Statuses[region.Name]
		status.Realms = config.SotahConfig.FilterInRealms(region, realms)
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
	itemIconsBase := store.NewItemIconsBase(stor, regions.USCentral1, gameversions.Retail)
	itemIconsBucket, err := itemIconsBase.GetFirmBucket()
	if err != nil {
		return ProdApiState{}, err
	}
	for i, prof := range apiState.Professions {
		itemIconUrl, err := func() (string, error) {
			obj := itemIconsBase.GetObject(prof.Icon, itemIconsBucket)
			exists, err := itemIconsBase.ObjectExists(obj)
			if err != nil {
				return "", err
			}

			url := fmt.Sprintf(
				store.ItemIconURLFormat,
				itemIconsBase.GetBucketName(),
				itemIconsBase.GetObjectName(prof.Icon),
			)

			if exists {
				return url, nil
			}

			body, err := util.Download(blizzard.DefaultGetItemIconURL(prof.Icon))
			if err != nil {
				return "", err
			}

			if err := itemIconsBase.Write(obj.NewWriter(stor.Context), body); err != nil {
				return "", err
			}

			return url, nil
		}()
		if err != nil {
			return ProdApiState{}, err
		}

		apiState.Professions[i].IconURL = itemIconUrl
	}

	// establishing bus-listeners
	apiState.BusListeners = NewBusListeners(SubjectBusListeners{
		subjects.Status: apiState.ListenForBusStatus,
	})

	// establishing messenger-listeners
	apiState.Listeners = NewListeners(SubjectListeners{
		subjects.Boot:                   apiState.ListenForMessengerBoot,
		subjects.Status:                 apiState.ListenForMessengerStatus,
		subjects.SessionSecret:          apiState.ListenForSessionSecret,
		subjects.ReceiveRealms:          apiState.ListenForReceiveRealms,
		subjects.RealmModificationDates: apiState.ListenForRealmModificationDates,
	})

	apiState.HellRegionRealms = hell.RegionRealmsMap{}

	return apiState, nil
}

type ProdApiState struct {
	State

	RealmsBase   store.RealmsBase
	RealmsBucket *storage.BucketHandle

	SessionSecret uuid.UUID
	ItemClasses   blizzard.ItemClasses
	Expansions    []sotah.Expansion
	Professions   []sotah.Profession
	ItemBlacklist ItemBlacklist

	BlizzardClientId     string
	BlizzardClientSecret string

	HellRegionRealms hell.RegionRealmsMap
}
