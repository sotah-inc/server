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

func apiTest(c *config, m messenger, dataDir string) error {
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
		statuses:  map[regionName]*status{},
		auctions:  map[regionName]map[realmSlug]*auctions{},
	}
	for _, reg := range c.Regions {
		// loading realm statuses
		stat, err := newStatus(reg, statusBody)
		if err != nil {
			return err
		}
		sta.statuses[reg.Name] = stat

		// loading realm auctions
		sta.auctions[reg.Name] = map[realmSlug]*auctions{}
		for _, rea := range stat.Realms {
			sta.auctions[reg.Name][rea.Slug] = auc
		}
	}

	// listening for status requests
	stopChans := map[string]chan interface{}{
		subjects.Status:            make(chan interface{}),
		subjects.Regions:           make(chan interface{}),
		subjects.GenericTestErrors: make(chan interface{}),
		subjects.Auctions:          make(chan interface{}),
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

func api(c *config, m messenger) error {
	log.Info("Starting api")

	// establishing a state
	resolver := newResolver(c)
	sta := state{
		messenger: m,
		resolver:  &resolver,
		regions:   c.Regions,
		statuses:  map[regionName]*status{},
		auctions:  map[regionName]map[realmSlug]*auctions{},
	}

	// gathering realms for each region
	for _, reg := range c.Regions {
		regionStatus, err := reg.getStatus(resolver)
		if err != nil {
			return err
		}

		sta.statuses[reg.Name] = regionStatus
	}

	// filling it with blank auctions
	for _, region := range c.Regions {
		sta.auctions[region.Name] = map[realmSlug]*auctions{}
	}

	// listening for status requests
	stopChans := map[string]chan interface{}{
		subjects.Status:            make(chan interface{}),
		subjects.Regions:           make(chan interface{}),
		subjects.GenericTestErrors: make(chan interface{}),
		subjects.Auctions:          make(chan interface{}),
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
