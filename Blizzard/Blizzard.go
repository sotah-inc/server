package Blizzard

import (
	"github.com/ihsw/go-download/Blizzard/Auction"
	"github.com/ihsw/go-download/Blizzard/AuctionData"
	"github.com/ihsw/go-download/Entity"
)

/*
	chan structs
*/
type Result struct {
	AuctionResponse     Auction.Response
	AuctionDataResponse AuctionData.Response
	DataUrl             string
	Error               error
	Realm               Entity.Realm
}

/*
	funcs
*/
func DownloadRealm(realm Entity.Realm, c chan Result) {
	result := Result{
		Realm: realm,
	}
	result.AuctionResponse, result.Error = Auction.Get(realm)
	if result.Error != nil {
		c <- result
		return
	}

	result.AuctionDataResponse, result.Error = AuctionData.Get(result.AuctionResponse.Files[0].Url)
	if result.Error != nil {
		c <- result
		return
	}

	c <- result
}
