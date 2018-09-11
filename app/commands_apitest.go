package main

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/logging"
	"github.com/ihsw/sotah-server/app/subjects"
	"github.com/ihsw/sotah-server/app/util"
	"github.com/sirupsen/logrus"
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
	auc, err := blizzard.NewAuctionsFromFilepath(fmt.Sprintf("%s/auctions.json", dataDirPath))
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
		auctions:  map[regionName]map[blizzard.RealmSlug]miniAuctionList{},
		items:     map[blizzard.ItemID]item{},
	}
	for _, reg := range c.Regions {
		// loading realm statuses
		stat, err := blizzard.NewStatus(statusBody)
		if err != nil {
			return err
		}
		sta.statuses[reg.Name] = status{Status: stat, region: reg, Realms: newRealms(reg, stat.Realms)}

		// misc
		regionItemIDsMap := map[blizzard.ItemID]struct{}{}

		// loading realm auctions
		sta.auctions[reg.Name] = map[blizzard.RealmSlug]miniAuctionList{}
		for _, rea := range stat.Realms {
			maList := newMiniAuctionListFromBlizzardAuctions(auc.Auctions)

			sta.auctions[reg.Name][rea.Slug] = maList

			for _, ID := range maList.itemIds() {
				_, ok := sta.items[ID]
				if ok {
					continue
				}

				regionItemIDsMap[ID] = struct{}{}
			}
		}

		// gathering the list of item IDs for this region
		regionItemIDs := make([]blizzard.ItemID, len(regionItemIDsMap))
		i := 0
		for ID := range regionItemIDsMap {
			regionItemIDs[i] = ID
			i++
		}

		// downloading items found in this region
		logging.WithField("items", len(regionItemIDs)).Info("Fetching items")
		itemsOut := getItems(regionItemIDs, sta.itemBlacklist, res)
		for job := range itemsOut {
			if job.err != nil {
				logging.WithFields(logrus.Fields{
					"region": reg.Name,
					"item":   job.ID,
					"error":  job.err.Error(),
				}).Error("Failed to fetch item")

				continue
			}

			sta.items[job.ID] = item{job.item, ""}
		}
		logging.WithField("items", len(regionItemIDs)).Info("Fetched items")
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
