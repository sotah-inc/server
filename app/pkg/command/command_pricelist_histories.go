package command

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/database"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state"
)

func PricelistHistories(config state.PricelistHistoriesStateConfig) error {
	logging.Info("Starting pricelist-histories")

	// establishing a state
	phState, err := state.NewPricelistHistoriesState(config)
	if err != nil {
		return err
	}

	// pruning old data
	earliestTime := database.DatabaseRetentionLimit()
	for _, reg := range phState.Regions {
		regionDatabaseDir := fmt.Sprintf("%s/%s", config.PricelistHistoriesDatabaseDir, reg.Name)

		for _, rea := range phState.Statuses[reg.Name].Realms {
			realmDatabaseDir := fmt.Sprintf("%s/%s", regionDatabaseDir, rea.Slug)
			dbPaths, err := database.DatabasePaths(realmDatabaseDir)
			if err != nil {
				logging.WithFields(logrus.Fields{
					"error": err.Error(),
					"dir":   realmDatabaseDir,
				}).Error("Failed to resolve database paths")

				return err
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

					return err
				}
			}
		}
	}

	// loading the pricelist-histories databases
	phDatabases, err := database.NewPricelistHistoryDatabases(config.PricelistHistoriesDatabaseDir, phState.Statuses)
	if err != nil {
		return err
	}
	phState.IO.Databases.PricelistHistoryDatabases = phDatabases

	// starting up a pruner
	logging.Info("Starting up the pricelist-histories file pruner")
	prunerStop := make(sotah.WorkerStopChan)
	onPrunerStop := phDatabases.StartPruner(prunerStop)

	// establishing listeners
	phState.Listeners = state.NewListeners(state.SubjectListeners{
		subjects.PriceListHistory:         phState.ListenForPriceListHistory,
		subjects.PricelistHistoriesIntake: phState.ListenForPricelistHistoriesIntake,
	})

	// opening all listeners
	logging.Info("Opening all listeners")
	if err := phState.Listeners.Listen(); err != nil {
		return err
	}

	// catching SIGINT
	logging.Info("Waiting for SIGINT")
	sigIn := make(chan os.Signal, 1)
	signal.Notify(sigIn, os.Interrupt)
	<-sigIn

	logging.Info("Caught SIGINT, exiting")

	// stopping listeners
	phState.Listeners.Stop()

	// stopping pruner
	logging.Info("Stopping pruner")
	prunerStop <- struct{}{}

	logging.Info("Waiting for pruner to stop")
	<-onPrunerStop

	logging.Info("Exiting")
	return nil
}
