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

/*
	error structs
*/
func RunQueue(formattedRealms []map[int64]Entity.Realm, downloadIn chan Entity.Realm, itemizeOut chan ItemizeResult, totalRealms int, cacheClient Cache.Client) (err error) {
	// populating the download queue
	for _, realms := range formattedRealms {
		for _, realm := range realms {
			downloadIn <- realm
		}
	}

	// waiting for the results to drain out
	results := make([]ItemizeResult, totalRealms)
	totalValidResults := 0
	for i := 0; i < totalRealms; i++ {
		result := <-itemizeOut
		if result.err != nil {
			err = errors.New(fmt.Sprintf("itemizeOut %s (%d) had an error (%s)", result.realm.Dump(), result.realm.Id, result.err.Error()))
			return
		}

		results[i] = result
		if !result.pass {
			totalValidResults++
		}
	}

	// gathering valid results
	validResults := make([]ItemizeResult, totalValidResults)
	i := 0
	for _, result := range results {
		if !result.pass {
			validResults[i] = result
			i++
		}
	}
	fmt.Println(fmt.Sprintf("Total results: %d", len(results)))
	fmt.Println(fmt.Sprintf("Valid results: %d", len(validResults)))
	// itemizeResults := ItemizeResults{list: validResults}

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
		auctionResponse     *Auction.Response
		auctionDataResponse *AuctionData.Response
		err                 error
	)
	realmManager := Entity.RealmManager{Client: cacheClient}
	result := DownloadResult{
		realm: realm,
		pass:  false,
	}

	// fetching the auction info
	auctionResponse, err = Auction.Get(realm, cacheClient.ApiKey)
	if err != nil {
		result.err = errors.New(fmt.Sprintf("Auction.Get() failed (%s)", err.Error()))
		out <- result
		return
	}

	// optionally halting on empty response
	if auctionResponse == nil {
		result.pass = true
		out <- result
		return
	}

	// fetching the actual auction data
	file := auctionResponse.Files[0]
	auctionDataResponse, err = AuctionData.Get(realm, file.Url)
	if err != nil {
		result.err = errors.New(fmt.Sprintf("AuctionData.Get() failed (%s)", err.Error()))
		out <- result
		return
	}

	// optionally halting on empty response
	if auctionDataResponse == nil {
		result.pass = true
		out <- result
		return
	}

	// loading it into the response
	result.auctionDataResponse = auctionDataResponse

	// flagging the realm as having been downloaded
	realm.LastDownloaded = time.Now()
	realmManager.Persist(realm)

	// queueing it out
	out <- result
}

func ItemizeRealm(downloadResult DownloadResult, cacheClient Cache.Client, out chan ItemizeResult) {
	// misc
	var err error
	realm := downloadResult.realm
	result := ItemizeResult{
		realm: realm,
		pass:  false,
	}

	// optionally halting on error
	if downloadResult.err != nil {
		result.err = errors.New(fmt.Sprintf("downloadResult had an error (%s)", downloadResult.err.Error()))
		out <- result
		return
	}

	// optionally halting due to pass up
	if downloadResult.pass {
		result.pass = true
		out <- result
		return
	}

	/*
		character handling
	*/
	characterManager := Character.Manager{Client: cacheClient}

	// gathering existing characters
	var existingCharacters []Character.Character
	existingCharacters, err = characterManager.FindByRealm(realm)
	if err != nil {
		result.err = errors.New(fmt.Sprintf("CharacterManager.FindByRealm() failed (%s)", err.Error()))
		out <- result
		return
	}

	// merging existing characters in and persisting them all
	result.characters, err = characterManager.PersistAll(realm, existingCharacters, downloadResult.getNewCharacters(existingCharacters))
	if err != nil {
		result.err = errors.New(fmt.Sprintf("CharacterManager.PersistAll() failed (%s)", err.Error()))
		out <- result
		return
	}

	/*
		item handling
	*/
	result.blizzItemIds = downloadResult.getBlizzItemIds()

	/*
		auction handling
	*/
	// gathering auctions for post-itemize processing
	result.auctions = downloadResult.auctionDataResponse.Auctions.Auctions

	// queueing it out
	out <- result
}
