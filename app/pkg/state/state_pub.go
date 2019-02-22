package state

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/metric"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/twinj/uuid"
)

type PubStateConfig struct {
	GCloudProjectID string

	MessengerHost string
	MessengerPort int

	PricelistHistoriesDatabaseV2Dir string
}

func NewPubStateIO(config PubStateConfig, statuses sotah.Statuses) (IO, error) {
	out := IO{}

	// establishing a bus
	logging.Info("Connecting bus-client")
	busClient, err := bus.NewClient(config.GCloudProjectID, "pub")
	out.BusClient = busClient

	// establishing a store
	logging.Info("Connecting store-client")
	stor, err := store.NewClient(config.GCloudProjectID)
	if err != nil {
		return IO{}, err
	}
	out.StoreClient = stor

	// loading the pricelist-histories-v2 databases
	logging.Info("Loading pricelist-history-databases-v2")
	phDatabases, err := database.NewPricelistHistoryDatabasesV2(config.PricelistHistoriesDatabaseV2Dir, statuses)
	if err != nil {
		return IO{}, err
	}
	out.Databases.PricelistHistoryDatabasesV2 = phDatabases

	return out, nil
}

func NewPubState(config PubStateConfig) (PubState, error) {
	// establishing an initial state
	pubState := PubState{
		State: NewState(uuid.NewV4(), true),
	}

	// connecting to the messenger host
	logging.Info("Connecting messenger")
	mess, err := messenger.NewMessenger(config.MessengerHost, config.MessengerPort)
	if err != nil {
		return PubState{}, err
	}
	pubState.IO.Messenger = mess

	// gathering regions
	logging.Info("Gathering regions")
	regions, err := pubState.NewRegions()
	if err != nil {
		return PubState{}, err
	}
	pubState.Regions = regions

	// gathering statuses
	logging.Info("Gathering statuses")
	for _, reg := range pubState.Regions {
		status, err := pubState.NewStatus(reg)
		if err != nil {
			return PubState{}, err
		}

		pubState.Statuses[reg.Name] = status
	}

	// pruning old data
	logging.Info("Checking for expired pricelist-histories-v2 databases")
	earliestTime := database.RetentionLimit()
	for regionName, status := range pubState.Statuses {
		regionDatabaseDir := fmt.Sprintf("%s/%s", config.PricelistHistoriesDatabaseV2Dir, regionName)

		for _, rea := range status.Realms {
			realmDatabaseDir := fmt.Sprintf("%s/%s", regionDatabaseDir, rea.Slug)
			dbPaths, err := database.DatabaseV2Paths(realmDatabaseDir)
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error": err.Error(),
					"dir":   realmDatabaseDir,
				}).Error("Failed to resolve database paths")

				return PubState{}, err
			}
			for _, dbPathPair := range dbPaths {
				if dbPathPair.TargetTime.After(earliestTime) {
					continue
				}

				logging.WithFields(logrus.Fields{
					"pathname": dbPathPair.FullPath,
				}).Debug("Pruning old pricelist-history database file")

				if err := os.Remove(dbPathPair.FullPath); err != nil {
					logging.WithFields(logrus.Fields{
						"error":    err.Error(),
						"dir":      realmDatabaseDir,
						"pathname": dbPathPair.FullPath,
					}).Error("Failed to remove database file")

					return PubState{}, err
				}
			}
		}
	}

	// connecting IO
	logging.Info("Connecting IO")
	io, err := NewPubStateIO(config, pubState.Statuses)
	if err != nil {
		return PubState{}, err
	}
	io.Messenger = mess
	io.Reporter = metric.NewReporter(mess)
	pubState.IO = io

	// establishing listeners
	logging.Info("Establishing listeners")
	pubState.Listeners = NewListeners(SubjectListeners{
		subjects.PricelistHistoriesIntakeV2: pubState.ListenForPricelistHistoriesIntakeV2,
	})

	return pubState, nil
}

type PubState struct {
	State
}
