package Work

import (
	"fmt"
	"github.com/ihsw/go-download/Blizzard/Auction"
	"github.com/ihsw/go-download/Blizzard/AuctionData"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Util"
)

/*
	chan structs
*/
type DownloadResult struct {
	AuctionResponse     Auction.Response
	AuctionDataResponse AuctionData.Response
	DataUrl             string
	Error               error
	Realm               Entity.Realm
}

type ItemizeResult struct {
	Error        error
	Realm        Entity.Realm
	AuctionCount int
}

/*
	funcs
*/
func DownloadRealm(realm Entity.Realm, out chan DownloadResult, output Util.Output) {
	result := DownloadResult{
		Realm: realm,
	}

	result.AuctionResponse, result.Error = Auction.Get(realm)
	if result.Error != nil {
		output.Write(fmt.Sprintf("Auction.Get() fail for %s: %s", realm.Dump(), result.Error.Error()))
		out <- result
		return
	}

	file := result.AuctionResponse.Files[0]

	output.Write(fmt.Sprintf("Start %s...", realm.Dump()))
	result.AuctionDataResponse, result.Error = AuctionData.Get(realm, file.Url)
	output.Write(fmt.Sprintf("Done %s...", realm.Dump()))
	if result.Error != nil {
		out <- result
		return
	}

	out <- result
}

func ItemizeRealm(downloadResult DownloadResult, out chan ItemizeResult) {
	realm := downloadResult.Realm
	result := ItemizeResult{
		Realm: realm,
	}

	if downloadResult.Error != nil {
		result.Error = downloadResult.Error
		out <- result
		return
	}

	data := downloadResult.AuctionDataResponse
	auctionCount := 0
	auctionGroups := [][]AuctionData.Auction{
		data.Alliance.Auctions,
		data.Horde.Auctions,
		data.Neutral.Auctions,
	}
	for _, auctions := range auctionGroups {
		auctionCount += len(auctions)
	}
	result.AuctionCount = auctionCount
	out <- result
}
