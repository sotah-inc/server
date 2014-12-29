package Work

import (
	"errors"
	"fmt"
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
		if result.Error != nil {
			err = errors.New(fmt.Sprintf("itemizeOut %s had an error (%s)", result.Realm.Dump(), result.Error.Error()))
			return
		}

		itemizeResults.List[i] = result
	}

	// gathering items from the results
	// newItems := itemizeResults.GetUniqueItems()

	// persisting them
	// itemManager := Entity.ItemManager{Client: cacheClient}
	// _, err = itemManager.PersistAll(newItems)
	// if err != nil {
	// 	return
	// }

	return nil
}

func DownloadRealm(realm Entity.Realm, cacheClient Cache.Client, out chan DownloadResult) {
	// misc
	var (
		auctionResponse Auction.Response
		err             error
	)
	realmManager := Entity.RealmManager{Client: cacheClient}
	result := DownloadResult{
		Realm:          realm,
		AlreadyChecked: false,
	}

	// fetching the auction info
	auctionResponse, err = Auction.Get(realm, cacheClient.ApiKey)
	if err != nil {
		result.Error = errors.New(fmt.Sprintf("Auction.Get() failed (%s)", err.Error()))
		out <- result
		return
	}

	// fetching the actual auction data
	file := auctionResponse.Files[0]
	result.AuctionDataResponse, err = AuctionData.Get(realm, file.Url)
	if err != nil {
		result.Error = errors.New(fmt.Sprintf("AuctionData.Get() failed (%s)", err.Error()))
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
		result.Error = errors.New(fmt.Sprintf("downloadResult had an error (%s)", downloadResult.Error.Error()))
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

	/*
		character handling
	*/
	characterManager := Character.Manager{Client: cacheClient}

	// gathering existing characters
	var existingCharacters []Character.Character
	existingCharacters, err = characterManager.FindByRealm(realm)
	if err != nil {
		result.Error = errors.New(fmt.Sprintf("CharacterManager.FindByRealm() failed (%s)", err.Error()))
		out <- result
		return
	}

	// gathering characters
	result.Characters, err = characterManager.PersistAll(realm, downloadResult.getCharacters(existingCharacters))
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
