package command

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/sotah-inc/server/app/pkg/database"

	"github.com/sotah-inc/server/app/pkg/state"

	"github.com/sotah-inc/server/app/internal"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/sotah-inc/server/app/pkg/util"
)

func apiTest(c internal.Config, m messenger.Messenger, s store.Store, dataDir string) error {
	logging.Info("Starting api-test")

	dataDirPath, err := filepath.Abs(dataDir)
	if err != nil {
		return err
	}

	// preloading the status file and auctions file
	statusBody, err := util.ReadFile(fmt.Sprintf("%s/realm-status.json", dataDirPath))
	if err != nil {
		return err
	}
	_, err = blizzard.NewAuctionsFromFilepath(fmt.Sprintf("%s/auctions.json", dataDirPath))
	if err != nil {
		return err
	}

	// establishing a state and filling it with statuses
	res := internal.NewResolver(c, m, s)
	sta := state.State{
		Messenger: m,
		Resolver:  res,
		Regions:   c.Regions,
		Statuses:  internal.Statuses{},
	}

	// loading up items database
	idBase, err := database.NewItemsDatabase(c)
	if err != nil {
		return err
	}
	sta.ItemsDatabase = idBase

	for _, reg := range c.Regions {
		// loading realm statuses
		stat, err := blizzard.NewStatus(statusBody)
		if err != nil {
			return err
		}
		sta.Statuses[reg.Name] = internal.Status{Status: stat, Region: reg, Realms: internal.NewRealms(reg, stat.Realms)}
	}

	// opening all listeners
	sta.Listeners = state.NewListeners(state.SubjectListeners{
		subjects.GenericTestErrors: sta.ListenForGenericTestErrors,
		subjects.Status:            sta.ListenForStatus,
		subjects.Regions:           sta.ListenForRegions,
		subjects.Auctions:          sta.ListenForAuctions,
		subjects.Owners:            sta.ListenForOwners,
		subjects.ItemsQuery:        sta.ListenForItemsQuery,
		subjects.ItemClasses:       sta.ListenForItemClasses,
		subjects.PriceList:         sta.ListenForPriceList,
		subjects.Items:             sta.ListenForItems,
	})
	if err := sta.Listeners.Listen(); err != nil {
		return err
	}

	// catching SIGINT
	logging.Info("Waiting for SIGINT")
	sigIn := make(chan os.Signal, 1)
	signal.Notify(sigIn, os.Interrupt)
	<-sigIn

	logging.Info("Caught SIGINT, exiting")

	// stopping listeners
	sta.Listeners.Stop()

	return nil
}
