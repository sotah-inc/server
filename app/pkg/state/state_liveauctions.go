package state

import (
	"fmt"

	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/diskstore"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/sotah-inc/server/app/pkg/util"
	"github.com/twinj/uuid"
)

type LiveAuctionsStateConfig struct {
	UseGCloud       bool
	GCloudProjectID string

	MessengerHost string
	MessengerPort int

	DiskStoreCacheDir string

	LiveAuctionsDatabaseDir string
}

func NewLiveAuctionsState(config LiveAuctionsStateConfig) (LiveAuctionsState, error) {
	laState := LiveAuctionsState{
		State: NewState(uuid.NewV4(), config.UseGCloud),
	}

	// connecting to the messenger host
	mess, err := messenger.NewMessenger(config.MessengerHost, config.MessengerPort)
	if err != nil {
		return LiveAuctionsState{}, err
	}
	laState.IO.Messenger = mess

	// gathering regions
	regions, err := laState.NewRegions()
	if err != nil {
		return LiveAuctionsState{}, err
	}
	laState.Regions = regions

	// gathering statuses
	for _, reg := range laState.Regions {
		status, err := laState.NewStatus(reg)
		if err != nil {
			return LiveAuctionsState{}, err
		}

		laState.Statuses[reg.Name] = status
	}

	// establishing a store (gcloud store or disk store)
	if config.UseGCloud {
		stor, err := store.NewStore(config.GCloudProjectID)
		if err != nil {
			return LiveAuctionsState{}, err
		}

		laState.IO.Store = stor
	} else {
		cacheDirs := []string{
			config.DiskStoreCacheDir,
			fmt.Sprintf("%s/auctions", config.DiskStoreCacheDir),
		}
		for _, reg := range laState.Regions {
			cacheDirs = append(cacheDirs, fmt.Sprintf("%s/auctions/%s", config.DiskStoreCacheDir, reg.Name))
		}
		if err := util.EnsureDirsExist(cacheDirs); err != nil {
			return LiveAuctionsState{}, err
		}

		laState.IO.DiskStore = diskstore.NewDiskStore(config.DiskStoreCacheDir)
	}

	// loading the live-auctions databases
	ladBases, err := database.NewLiveAuctionsDatabases(config.LiveAuctionsDatabaseDir, laState.Statuses)
	if err != nil {
		return LiveAuctionsState{}, err
	}
	laState.IO.Databases.LiveAuctionsDatabases = ladBases

	return laState, nil
}

type LiveAuctionsState struct {
	State
}
