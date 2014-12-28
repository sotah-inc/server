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
