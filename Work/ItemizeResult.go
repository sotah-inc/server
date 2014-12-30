package Work

import (
	"github.com/ihsw/go-download/Blizzard/AuctionData"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Entity/Character"
)

type ItemizeResult struct {
	err          error
	realm        Entity.Realm
	blizzItemIds []int64
	characters   []Character.Character
	auctions     []AuctionData.Auction
	pass         bool
}
