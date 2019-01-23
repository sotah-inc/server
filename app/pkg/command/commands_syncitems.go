package command

import (
	"fmt"

	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/util"
)

func syncItems(c config, s store) error {
	logging.Info("Starting sync-items")

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
