package Blizzard

import (
	"github.com/ihsw/go-download/Blizzard/Auction"
	_ "github.com/ihsw/go-download/Blizzard/AuctionData"
	"github.com/ihsw/go-download/Entity"
)

func DownloadRealm(realm Entity.Realm, c chan Auction.Result) {
	c <- Auction.Get(realm)
}
