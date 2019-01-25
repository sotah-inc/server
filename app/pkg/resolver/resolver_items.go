package resolver

import (
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/util"
)

func (r Resolver) GetItem(primaryRegion sotah.Region, ID blizzard.ItemID) (blizzard.Item, []byte, error) {
	uri, err := r.AppendAccessToken(r.GetItemURL(primaryRegion.Hostname, ID))
	if err != nil {
		return blizzard.Item{}, []byte{}, err
	}

	item, resp, err := blizzard.NewItemFromHTTP(uri)
	if resp.Status == 404 {
		return blizzard.Item{}, []byte{}, nil
	}
	if err != nil {
		return blizzard.Item{}, []byte{}, err
	}

	return item, resp.Body, nil
}

type GetItemsJob struct {
	Err    error
	ID     blizzard.ItemID
	Item   blizzard.Item
	Exists bool

	GzipDecodedData []byte
}

func (r Resolver) GetItems(primaryRegion sotah.Region, IDs []blizzard.ItemID) chan GetItemsJob {
	// establishing channels
	out := make(chan GetItemsJob)
	in := make(chan blizzard.ItemID)

	// spinning up the workers for fetching items
	worker := func() {
		for ID := range in {
			itemValue, gzipDecodedData, err := r.GetItem(primaryRegion, ID)
			exists := itemValue.ID > 0
			out <- GetItemsJob{err, ID, itemValue, exists, gzipDecodedData}
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
