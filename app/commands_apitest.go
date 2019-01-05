package main

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/sotah-inc/server/app/blizzard"
	"github.com/sotah-inc/server/app/logging"
	"github.com/sotah-inc/server/app/subjects"
	"github.com/sotah-inc/server/app/util"
)

func apiTest(c config, m messenger, s store, dataDir string) error {
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
	res := newResolver(c, m, s)
	sta := state{
		messenger: m,
		resolver:  res,
		regions:   c.Regions,
		statuses:  map[regionName]status{},
	}

	// loading up items database
	idBase, err := newItemsDatabase(c)
	if err != nil {
		return err
	}
	sta.itemsDatabase = idBase

	for _, reg := range c.Regions {
		// loading realm statuses
		stat, err := blizzard.NewStatus(statusBody)
		if err != nil {
			return err
		}
		sta.statuses[reg.Name] = status{Status: stat, region: reg, Realms: newRealms(reg, stat.Realms)}
	}

	// opening all listeners
	sta.listeners = newListeners(subjectListeners{
		subjects.GenericTestErrors: sta.listenForGenericTestErrors,
		subjects.Status:            sta.listenForStatus,
		subjects.Regions:           sta.listenForRegions,
		subjects.Auctions:          sta.listenForAuctions,
		subjects.Owners:            sta.listenForOwners,
		subjects.ItemsQuery:        sta.listenForItemsQuery,
		subjects.ItemClasses:       sta.listenForItemClasses,
		subjects.PriceList:         sta.listenForPriceList,
		subjects.Items:             sta.listenForItems,
	})
	if err := sta.listeners.listen(); err != nil {
		return err
	}

	// catching SIGINT
	logging.Info("Waiting for SIGINT")
	sigIn := make(chan os.Signal, 1)
	signal.Notify(sigIn, os.Interrupt)
	<-sigIn

	logging.Info("Caught SIGINT, exiting")

	// stopping listeners
	sta.listeners.stop()

	return nil
}
