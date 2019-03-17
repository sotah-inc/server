package state

import (
	"fmt"

	"github.com/sotah-inc/server/app/pkg/diskstore"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/util"
	"github.com/twinj/uuid"
)

type PricelistHistoriesStateConfig struct {
	MessengerHost string
	MessengerPort int

	DiskStoreCacheDir string

	PricelistHistoriesDatabaseDir string
}

func NewPricelistHistoriesState(config PricelistHistoriesStateConfig) (PricelistHistoriesState, error) {
	phState := PricelistHistoriesState{
		State: NewState(uuid.NewV4(), false),
	}

	// connecting to the messenger host
	mess, err := messenger.NewMessenger(config.MessengerHost, config.MessengerPort)
	if err != nil {
		return PricelistHistoriesState{}, err
	}
	phState.IO.Messenger = mess

	// initializing a reporter
	phState.IO.Reporter = metric.NewReporter(mess)

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

	// establishing a store
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

	return phState, nil
}

type PricelistHistoriesState struct {
	State
}
