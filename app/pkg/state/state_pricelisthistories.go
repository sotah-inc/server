package state

import (
	"fmt"

	"github.com/sotah-inc/server/app/pkg/diskstore"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/sotah-inc/server/app/pkg/util"
	"github.com/twinj/uuid"
)

type PricelistHistoriesStateConfig struct {
	UseGCloud       bool
	GCloudProjectID string

	MessengerHost string
	MessengerPort int

	DiskStoreCacheDir string

	PricelistHistoriesDatabaseDir string
}

func NewPricelistHistoriesState(config PricelistHistoriesStateConfig) (PricelistHistoriesState, error) {
	phState := PricelistHistoriesState{
		State: NewState(uuid.NewV4(), config.UseGCloud),
	}

	// connecting to the messenger host
	mess, err := messenger.NewMessenger(config.MessengerHost, config.MessengerPort)
	if err != nil {
		return PricelistHistoriesState{}, err
	}
	phState.IO.Messenger = mess

	// gathering regions
	regions, err := phState.NewRegions()
	if err != nil {
		return PricelistHistoriesState{}, err
	}
	phState.Regions = regions

	// gathering statuses
	for _, reg := range phState.Regions {
		status, err := phState.NewStatus(reg)
		if err != nil {
			return PricelistHistoriesState{}, err
		}

		phState.Statuses[reg.Name] = status
	}

	// establishing a store (gcloud store or disk store)
	if config.UseGCloud {
		stor, err := store.NewStore(config.GCloudProjectID)
		if err != nil {
			return PricelistHistoriesState{}, err
		}

		phState.IO.Store = stor
	} else {
		cacheDirs := []string{
			config.DiskStoreCacheDir,
			fmt.Sprintf("%s/auctions", config.DiskStoreCacheDir),
		}
		for _, reg := range phState.Regions {
			cacheDirs = append(cacheDirs, fmt.Sprintf("%s/auctions/%s", config.DiskStoreCacheDir, reg.Name))
		}
		if err := util.EnsureDirsExist(cacheDirs); err != nil {
			return PricelistHistoriesState{}, err
		}

		phState.IO.DiskStore = diskstore.NewDiskStore(config.DiskStoreCacheDir)
	}

	return phState, nil
}

type PricelistHistoriesState struct {
	State
}
