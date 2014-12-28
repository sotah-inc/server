package Work

import (
	"github.com/ihsw/go-download/Blizzard/AuctionData"
	"github.com/ihsw/go-download/Entity"
)

type ItemizeResult struct {
	Error        error
	Realm        Entity.Realm
	BlizzItemIds []int64
	Characters   []Entity.Character
	Auctions     []AuctionData.Auction
}

/*
	ItemizeResults
*/
type ItemizeResults struct {
	List []ItemizeResult
}

func (self ItemizeResults) GetUniqueItems() (items []Entity.Item) {
	blizzItemIds := make(map[int64]struct{})
	for _, result := range self.List {
		for _, blizzItemId := range result.BlizzItemIds {
			_, valid := blizzItemIds[blizzItemId]
			if !valid {
				blizzItemIds[blizzItemId] = struct{}{}
			}
		}
	}

	items = make([]Entity.Item, len(blizzItemIds))
	i := 0
	for blizzItemId, _ := range blizzItemIds {
		items[i] = Entity.Item{BlizzId: blizzItemId}
		i++
	}

	return items
}
