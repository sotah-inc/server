package main

import (
	"fmt"

	"github.com/ihsw/sotah-server/app/util"
	log "github.com/sirupsen/logrus"
)

func syncItems(c config, s store) error {
	log.Info("Starting sync-items")

	// ensuring items dir exists
	if err := util.EnsureDirsExist([]string{fmt.Sprintf("%s/items", c.CacheDir)}); err != nil {
		return err
	}

	exportedItems := s.exportItems()
	for job := range exportedItems {
		itemFilepath, err := getItemFilepath(c, job.ID)
		if err != nil {
			return err
		}

		if err := util.WriteFile(itemFilepath, job.data); err != nil {
			return err
		}
	}

	return nil
}
