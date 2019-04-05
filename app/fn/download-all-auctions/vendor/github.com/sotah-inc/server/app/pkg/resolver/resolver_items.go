package resolver

import (
	"net/http"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/util"
)

func (r Resolver) NewItem(primaryRegion sotah.Region, ID blizzard.ItemID) (blizzard.Item, error) {
	resp, err := r.Download(r.GetItemURL(primaryRegion.Hostname, ID), true)
	if err != nil {
		return blizzard.Item{}, err
	}
	if resp.Status == http.StatusNotFound {
		return blizzard.Item{}, nil
	}

	return blizzard.NewItem(resp.Body)
}

type GetItemsJob struct {
	Err    error
	ItemId blizzard.ItemID
	Item   blizzard.Item
	Exists bool
}

func (r Resolver) GetItems(primaryRegion sotah.Region, IDs []blizzard.ItemID) chan GetItemsJob {
	// establishing channels
	out := make(chan GetItemsJob)
	in := make(chan blizzard.ItemID)

	// spinning up the workers for fetching items
	worker := func() {
		for itemId := range in {
			item, err := r.NewItem(primaryRegion, itemId)
			if err != nil {
				out <- GetItemsJob{
					Err:    err,
					Item:   blizzard.Item{},
					Exists: false,
					ItemId: 0,
				}

				continue
			}

			out <- GetItemsJob{
				ItemId: itemId,
				Exists: item.ID > 0,
				Item:   item,
				Err:    nil,
			}
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
