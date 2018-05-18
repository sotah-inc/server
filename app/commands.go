package main

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/ihsw/sotah-server/app/subjects"
	"github.com/ihsw/sotah-server/app/util"
	log "github.com/sirupsen/logrus"
)

func apiTest(c config, m messenger, dataDir string) error {
	log.Info("Starting api-test")

	dataDirPath, err := filepath.Abs(dataDir)
	if err != nil {
		return err
	}

	// preloading the status file and auctions file
	statusBody, err := util.ReadFile(fmt.Sprintf("%s/realm-status.json", dataDirPath))
	if err != nil {
		return err
	}
	auc, err := newAuctionsFromFilepath(fmt.Sprintf("%s/auctions.json", dataDirPath))
	if err != nil {
		return err
	}

	// establishing a state and filling it with statuses
	sta := state{
		messenger: m,
		regions:   c.Regions,
		statuses:  map[regionName]status{},
		auctions:  map[regionName]map[realmSlug]miniAuctionList{},
	}
	for _, reg := range c.Regions {
		// loading realm statuses
		stat, err := newStatus(reg, statusBody)
		if err != nil {
			return err
		}
		sta.statuses[reg.Name] = stat

		// loading realm auctions
		sta.auctions[reg.Name] = map[realmSlug]miniAuctionList{}
		for _, rea := range stat.Realms {
			sta.auctions[reg.Name][rea.Slug] = auc.Auctions.minimize()
		}
	}

	// listening for status requests
	stopChans := map[subjects.Subject]chan interface{}{
		subjects.Status:            make(chan interface{}),
		subjects.Regions:           make(chan interface{}),
		subjects.GenericTestErrors: make(chan interface{}),
		subjects.Auctions:          make(chan interface{}),
		subjects.Owners:            make(chan interface{}),
	}
	if err := sta.listenForStatus(stopChans[subjects.Status]); err != nil {
		return err
	}
	if err := sta.listenForRegions(stopChans[subjects.Regions]); err != nil {
		return err
	}
	if err := sta.listenForGenericTestErrors(stopChans[subjects.GenericTestErrors]); err != nil {
		return err
	}
	if err := sta.listenForAuctions(stopChans[subjects.Auctions]); err != nil {
		return err
	}
	if err := sta.listenForOwners(stopChans[subjects.Owners]); err != nil {
		return err
	}

	// catching SIGINT
	sigIn := make(chan os.Signal, 1)
	signal.Notify(sigIn, os.Interrupt)
	<-sigIn

	log.Info("Caught SIGINT, exiting")

	// stopping listeners
	for _, stop := range stopChans {
		stop <- struct{}{}
	}

	return nil
}

func api(c config, m messenger) error {
	log.Info("Starting api")

	// establishing a state
	resolver := newResolver(c)
	sta := state{
		messenger: m,
		resolver:  resolver,
		regions:   c.Regions,
		statuses:  map[regionName]status{},
		auctions:  map[regionName]map[realmSlug]miniAuctionList{},
		items:     map[itemID]item{},
	}

	// ensuring auctions cache-dir exists
	err := util.EnsureDirExists(fmt.Sprintf("%s/auctions", c.CacheDir))
	if err != nil {
		return err
	}

	// ensuring items cache-dir exists
	err = util.EnsureDirExists(fmt.Sprintf("%s/items", c.CacheDir))
	if err != nil {
		return err
	}

	// ensuring each region-auctions cache-dir exists
	for _, reg := range c.Regions {
		err := util.EnsureDirExists(fmt.Sprintf("%s/auctions/%s", c.CacheDir, reg.Name))
		if err != nil {
			return err
		}
	}

	// filling state with region statuses and a blank list of auctions
	for _, reg := range c.Regions {
		regionStatus, err := reg.getStatus(resolver)
		if err != nil {
			return err
		}

		sta.statuses[reg.Name] = regionStatus

		sta.auctions[reg.Name] = map[realmSlug]miniAuctionList{}
		for _, rea := range regionStatus.Realms {
			sta.auctions[reg.Name][rea.Slug] = miniAuctionList{}
		}
	}

	// listening for status requests
	stopChans := map[subjects.Subject]chan interface{}{
		subjects.Status:            make(chan interface{}),
		subjects.Regions:           make(chan interface{}),
		subjects.GenericTestErrors: make(chan interface{}),
		subjects.Auctions:          make(chan interface{}),
		subjects.Owners:            make(chan interface{}),
		subjects.Items:             make(chan interface{}),
	}
	if err := sta.listenForStatus(stopChans[subjects.Status]); err != nil {
		return err
	}
	if err := sta.listenForRegions(stopChans[subjects.Regions]); err != nil {
		return err
	}
	if err := sta.listenForGenericTestErrors(stopChans[subjects.GenericTestErrors]); err != nil {
		return err
	}
	if err := sta.listenForAuctions(stopChans[subjects.Auctions]); err != nil {
		return err
	}
	if err := sta.listenForOwners(stopChans[subjects.Owners]); err != nil {
		return err
	}
	if err := sta.listenForItems(stopChans[subjects.Items]); err != nil {
		return err
	}

	// going over the list of regions
	for _, reg := range sta.regions {
		// resolving the realm whitelist
		var whitelist getAuctionsWhitelist
		whitelist = nil
		if _, ok := c.Whitelist[reg.Name]; ok {
			whitelist = c.Whitelist[reg.Name]
		}

		// misc
		regionItemIDsMap := map[itemID]struct{}{}

		// downloading auctions in a region
		log.WithFields(log.Fields{
			"region":    reg.Name,
			"realms":    len(sta.statuses[reg.Name].Realms),
			"whitelist": whitelist,
		}).Info("Downloading region")
		auctionsOut := sta.statuses[reg.Name].Realms.getAuctionsOrAll(sta.resolver, whitelist)
		for job := range auctionsOut {
			itemIDs := sta.auctionsIntake(job)
			for _, ID := range itemIDs {
				_, ok := sta.items[ID]
				if ok {
					continue
				}

				regionItemIDsMap[ID] = struct{}{}
			}
		}
		log.WithField("region", reg.Name).Info("Downloaded region")

		// gathering the list of item IDs for this region
		regionItemIDs := make([]itemID, len(regionItemIDsMap))
		i := 0
		for ID := range regionItemIDsMap {
			regionItemIDs[i] = ID
			i++
		}

		// downloading items found in this region
		log.WithField("items", len(regionItemIDs)).Info("Fetching items")
		itemsOut := getItems(regionItemIDs, resolver)
		for job := range itemsOut {
			if job.err != nil {
				log.WithFields(log.Fields{
					"region": reg.Name,
					"item":   job.ID,
					"error":  job.err.Error(),
				}).Info("Failed to fetch item")

				continue
			}

			sta.items[job.ID] = job.item
		}
		log.WithField("items", len(regionItemIDs)).Info("Fetched items")
	}

	// catching SIGINT
	sigIn := make(chan os.Signal, 1)
	signal.Notify(sigIn, os.Interrupt)
	<-sigIn

	log.Info("Caught SIGINT, exiting")

	// stopping listeners
	for _, stop := range stopChans {
		stop <- struct{}{}
	}

	return nil
}
