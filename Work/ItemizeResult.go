package Work

import (
	"github.com/ihsw/go-download/Blizzard/AuctionData"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Entity/Character"
)

type ItemizeResult struct {
	Error          error
	Realm          Entity.Realm
	BlizzItemIds   []int64
	Characters     []Character.Character
	Auctions       []AuctionData.Auction
	AlreadyChecked bool
}
