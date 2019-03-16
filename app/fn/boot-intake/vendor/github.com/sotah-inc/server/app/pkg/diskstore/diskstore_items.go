package diskstore

import (
	"fmt"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/util"
	"os"
)

func (ds DiskStore) resolveItemFilepath(ID blizzard.ItemID) string {
	return fmt.Sprintf("%s/items/%d.json", ds.CacheDir, ID)
}

func (ds DiskStore) WriteItem(ID blizzard.ItemID, data []byte) error {
	return util.WriteFile(ds.resolveItemFilepath(ID), data)
}

func (ds DiskStore) NewItem(ID blizzard.ItemID) (blizzard.Item, error) {
	itemFilepath := ds.resolveItemFilepath(ID)
	if _, err := os.Stat(itemFilepath); err != nil {
		if !os.IsNotExist(err) {
			return blizzard.Item{}, err
		}

		return blizzard.Item{}, nil
	}

	return blizzard.NewItemFromFilepath(itemFilepath)
}

type GetItemsJob struct {
	Err    error
	ID     blizzard.ItemID
	Item   blizzard.Item
	Exists bool
}

func (ds DiskStore) GetItems(IDs []blizzard.ItemID) chan GetItemsJob {
	// establishing channels
	out := make(chan GetItemsJob)
	in := make(chan blizzard.ItemID)

	// spinning up the workers for fetching items
	worker := func() {
		for ID := range in {
			itemValue, err := ds.NewItem(ID)
			exists := itemValue.ID > 0
			out <- GetItemsJob{err, ID, itemValue, exists}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(8, worker, postWork)

	// queueing up the items
	go func() {
		for _, ID := range IDs {
			in <- ID
		}

		close(in)
	}()

	return out
}
