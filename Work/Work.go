package Work

import (
	"github.com/ihsw/go-download/Blizzard/Auction"
	"github.com/ihsw/go-download/Blizzard/AuctionData"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Entity"
	"time"
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
func DownloadRealm(realm Entity.Realm, out chan DownloadResult, cacheClient Cache.Client) {
	// misc
	realmManager := Entity.RealmManager{Client: cacheClient}
	result := DownloadResult{
		Realm: realm,
	}

	// fetching the auction info
	result.AuctionResponse, result.Error = Auction.Get(realm)
	if result.Error != nil {
		out <- result
		return
	}

	// fetching the actual auction data
	file := result.AuctionResponse.Files[0]
	result.AuctionDataResponse, result.Error = AuctionData.Get(realm, file.Url)
	if result.Error != nil {
		out <- result
		return
	}

	// flagging the realm as having been downloaded
	realm.LastDownloaded = time.Now()
	realmManager.Persist(realm)

	// queueing it out
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
