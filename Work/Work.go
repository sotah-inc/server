package Work

import (
	"github.com/ihsw/go-download/Blizzard/Auction"
	"github.com/ihsw/go-download/Blizzard/AuctionData"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Entity/Character"
	"time"
)

func RunQueue(formattedRealms []map[int64]Entity.Realm, downloadIn chan Entity.Realm, itemizeOut chan ItemizeResult, totalRealms int, cacheClient Cache.Client) (err error) {
	/*
		populating the download queue
	*/
	// pushing the realms into the start of the queue
	for _, realms := range formattedRealms {
		for _, realm := range realms {
			downloadIn <- realm
		}
	}

	/*
		reading the itemize queue
	*/
	// waiting for the results to drain out
	itemizeResults := ItemizeResults{List: make([]ItemizeResult, totalRealms)}
	for i := 0; i < totalRealms; i++ {
		result := <-itemizeOut
		if err = result.Error; err != nil {
			return
		}

		itemizeResults.List[i] = result
	}

	// gathering items from the results
	newItems := itemizeResults.GetUniqueItems()

	// persisting them
	itemManager := Entity.ItemManager{Client: cacheClient}
	_, err = itemManager.PersistAll(newItems)
	if err != nil {
		return
	}

	return nil
}

func DownloadRealm(realm Entity.Realm, cacheClient Cache.Client, out chan DownloadResult) {
	// misc
	var auctionResponse Auction.Response
	realmManager := Entity.RealmManager{Client: cacheClient}
	result := DownloadResult{
		Realm:          realm,
		AlreadyChecked: false,
	}

	// fetching the auction info
	auctionResponse, result.Error = Auction.Get(realm, cacheClient.ApiKey)
	if result.Error != nil {
		out <- result
		return
	}

	// fetching the actual auction data
	file := auctionResponse.Files[0]
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
	var err error
	realm := downloadResult.Realm
	result := ItemizeResult{
		Realm:          realm,
		AlreadyChecked: false,
	}

	// optionally halting on error
	if downloadResult.Error != nil {
		result.Error = downloadResult.Error
		out <- result
		return
	}

	// optionally halting on already having been checked
	if downloadResult.AlreadyChecked {
		result.AlreadyChecked = true
		out <- result
		return
	}

	// gathering blizz-item-ids for post-itemize processing
	result.BlizzItemIds = downloadResult.getBlizzItemIds()

	// gathering characters and persisting them
	characters := downloadResult.getCharacters()
	characterManager := Character.Manager{Client: cacheClient}
	result.Characters, err = characterManager.PersistAll(realm, characters)
	if err != nil {
		result.Error = err
		out <- result
		return
	}

	// gathering auctions for post-itemize processing
	result.Auctions = downloadResult.AuctionDataResponse.Auctions.Auctions

	// queueing it out
	out <- result
}
