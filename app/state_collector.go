package main

import (
	"time"

	log "github.com/sirupsen/logrus"
)

func (sta state) startCollector(stopChan workerStopChan, res resolver) workerStopChan {
	// collecting regions once
	sta.collectRegions(res)

	onStop := make(workerStopChan)
	go func() {
		ticker := time.NewTicker(10 * time.Minute)

		log.Info("Starting collector")
	outer:
		for {
			select {
			case <-ticker.C:
				sta.collectRegions(res)
			case <-stopChan:
				ticker.Stop()

				break outer
			}
		}

		onStop <- struct{}{}
	}()

	return onStop
}
