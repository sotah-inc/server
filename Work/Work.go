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
	BlizzItemIds []uint64
}

/*
	funcs
*/
func DownloadRealm(realm Entity.Realm, cacheClient Cache.Client, out chan DownloadResult) {
	// misc
	realmManager := Entity.RealmManager{Client: cacheClient}
	result := DownloadResult{
		Realm: realm,
	}

	// fetching the auction info
	result.AuctionResponse, result.Error = Auction.Get(realm, cacheClient.ApiKey)
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

func ItemizeRealm(downloadResult DownloadResult, cacheClient Cache.Client, out chan ItemizeResult) {
	// misc
	realm := downloadResult.Realm
	result := ItemizeResult{
		Realm: realm,
	}

	// optionally halting on error
	if downloadResult.Error != nil {
		result.Error = downloadResult.Error
		out <- result
		return
	}

	// going over the list of auctions to gather the blizz item ids
	data := downloadResult.AuctionDataResponse
	auctionGroups := [][]AuctionData.Auction{
		data.Alliance.Auctions,
		data.Horde.Auctions,
		data.Neutral.Auctions,
	}
	blizzItemIds := make(map[uint64]struct{})
	for _, auctions := range auctionGroups {
		for _, auction := range auctions {
			blizzItemId := auction.Item
			_, valid := blizzItemIds[blizzItemId]
			if !valid {
				blizzItemIds[blizzItemId] = struct{}{}
			}
		}
	}

	result.BlizzItemIds = make([]uint64, len(blizzItemIds))
	i := 0
	for blizzItemId, _ := range blizzItemIds {
		result.BlizzItemIds[i] = blizzItemId
		i++
	}

	// queueing it out
	out <- result
}
