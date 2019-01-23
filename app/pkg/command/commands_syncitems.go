package command

import (
	"fmt"

	"github.com/sotah-inc/server/app/internal"
	"github.com/sotah-inc/server/app/pkg/store"

	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/util"
)

func syncItems(c internal.Config, s store.Store) error {
	logging.Info("Starting sync-items")

	// ensuring items dir exists
	if err := util.EnsureDirsExist([]string{fmt.Sprintf("%s/items", c.CacheDir)}); err != nil {
		return err
	}

	exportedItems := s.ExportItems()
	for job := range exportedItems {
		itemFilepath, err := internal.GetItemFilepath(c, job.ID)
		if err != nil {
			return err
		}

		if err := util.WriteFile(itemFilepath, job.Data); err != nil {
			return err
		}
	}

	return nil
}
