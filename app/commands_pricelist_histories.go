package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/ihsw/sotah-server/app/subjects"
	log "github.com/sirupsen/logrus"
)

func pricelistHistories(c config, m messenger, s store) error {
	log.Info("Starting pricelist-histories")

	// establishing a state
	res := newResolver(c, m, s)
	sta := newState(m, res)

	// gathering region-status from the root service
	regions := []*region{}
	attempts := 0
	for {
		var err error
		regions, err = newRegionsFromMessenger(m)
		if err == nil {
			break
		} else {
			log.Info("Could not fetch regions, retrying in 250ms")

			attempts++
			time.Sleep(250 * time.Millisecond)
		}

		if attempts >= 20 {
			return fmt.Errorf("Failed to fetch regions after %d attempts", attempts)
		}
	}
	for i, reg := range regions {
		sta.regions[i] = *reg

		stas, err := newStatusFromMessenger(*reg, m)
		if err != nil {
			return err
		}

		sta.statuses[reg.Name] = stas
	}

	// loading up databases
	// dbs, err := newDatabases(c, sta.statuses, itemIds)
	// if err != nil {
	// 	return err
	// }
	// sta.databases = dbs

	// opening all listeners
	sta.listeners = newListeners(subjectListeners{
		subjects.PricelistsIntake: sta.listenForAuctions,
	})
	if err := sta.listeners.listen(); err != nil {
		return err
	}

	// catching SIGINT
	log.Info("Waiting for SIGINT")
	sigIn := make(chan os.Signal, 1)
	signal.Notify(sigIn, os.Interrupt)
	<-sigIn

	log.Info("Caught SIGINT, exiting")

	// stopping listeners
	sta.listeners.stop()

	log.Info("Exiting")
	return nil
}
