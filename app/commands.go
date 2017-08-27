package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/ihsw/sotah-server/app/subjects"
)

func apiTest(c *config, m messenger) error {
	// establishing a state and filling it with statuses
	sta := state{
		messenger: m,
		config:    c,
		statuses:  map[regionName]*status{},
	}
	for _, reg := range c.Regions {
		stat, err := newStatusFromFilepath(reg, "./src/github.com/ihsw/sotah-server/app/TestData/realm-status.json")
		if err != nil {
			return err
		}

		sta.statuses[reg.Name] = stat
	}

	// listening for status requests
	stopChans := map[string]chan interface{}{
		subjects.Status:            make(chan interface{}),
		subjects.Regions:           make(chan interface{}),
		subjects.GenericTestErrors: make(chan interface{}),
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

	fmt.Printf("Running!\n")

	// catching SIGINT
	sigIn := make(chan os.Signal, 1)
	signal.Notify(sigIn, os.Interrupt)
	<-sigIn
	fmt.Printf("Caught SIGINT!\n")

	// stopping listeners
	for _, stop := range stopChans {
		stop <- struct{}{}
	}

	return nil
}
