package Work

import (
	"errors"
	"fmt"
	"github.com/ihsw/go-download/Blizzard/Auction"
	"github.com/ihsw/go-download/Blizzard/AuctionData"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Entity/Character"
	"github.com/ihsw/go-download/Util"
	"time"
)

type Queue struct {
	DownloadIn  chan Entity.Realm
	DownloadOut chan DownloadResult
	ItemizeOut  chan ItemizeResult
	CacheClient Cache.Client
}

func (self Queue) DownloadRealms(regionRealms map[int64][]Entity.Realm, totalRealms int) (map[int64][]Entity.Realm, error) {
	var err error

	// formatting the realms to be evenly distributed
	largestRegion := 0
	for _, realms := range regionRealms {
		if len(realms) > largestRegion {
			largestRegion = len(realms)
		}
	}
	formattedRealms := make([]map[int64]Entity.Realm, largestRegion)
	for regionId, realms := range regionRealms {
		for i, realm := range realms {
			if formattedRealms[int64(i)] == nil {
				formattedRealms[int64(i)] = map[int64]Entity.Realm{}
			}
			formattedRealms[int64(i)][regionId] = realm
		}
	}

	// populating the download queue
	for _, realms := range formattedRealms {
		for _, realm := range realms {
			self.DownloadIn <- realm
		}
	}

	// waiting for the results to drain out
	results := []ItemizeResult{}
	startTime := time.Now()
	for i := 0; i < totalRealms; i++ {
		result := <-self.ItemizeOut

		// optionally halting on error
		if result.Err != nil {
			err = errors.New(fmt.Sprintf("itemizeOut %s (%d) had an error (%s)", result.realm.Dump(), result.realm.Id, result.Err.Error()))
			return regionRealms, err
		}

		if result.responseFailed {
			continue
		}

		if result.AlreadyChecked {
			continue
		}

		results = append(results, result)
	}

	// dumping the duration
	duration := time.Since(startTime).Seconds()
	fmt.Println(fmt.Sprintf("Finished in %.2fs!", duration))

	// refresing the region-realms list
	for _, result := range results {
		resultRealm := result.realm
		resultRegion := resultRealm.Region
		for i, realm := range regionRealms[resultRegion.Id] {
			if realm.Id != resultRealm.Id {
				continue
			}

			regionRealms[result.realm.Region.Id][i] = resultRealm
		}
	}

	// calculating the earliest last-modified
	earliestRealm := Entity.Realm{}
	for _, result := range results {
		realm := result.realm
		if earliestRealm.LastDownloaded.IsZero() || realm.LastDownloaded.Before(earliestRealm.LastDownloaded) {
			earliestRealm = realm
		}
	}
	if earliestRealm.IsValid() {
		fmt.Println(fmt.Sprintf("Earliest realm: %s, last-modified: %s", earliestRealm.Dump(), earliestRealm.LastDownloaded.Format(Util.WriteLayout)))
	} else {
		fmt.Println("There is no new realm data!")
	}

	/*
		item handling
	*/
	// misc
	itemManager := Entity.ItemManager{Client: self.CacheClient}

	// gathering existing items
	var existingItems []Entity.Item
	existingItems, err = itemManager.FindAll()

	// gathering new items
	itemizeResults := ItemizeResults{list: results}
	newItems := itemizeResults.getNewItems(existingItems)

	fmt.Println(fmt.Sprintf("Existing items: %d", len(existingItems)))
	fmt.Println(fmt.Sprintf("New items: %d", len(newItems)))

	// persisting them
	newItems, err = itemManager.PersistAll(newItems)
	if err != nil {
		return regionRealms, err
	}

	// clearing the cache-client cache
	self.CacheClient.ClearCaches()

	return regionRealms, nil
}

func (self Queue) DownloadRealm(realm Entity.Realm) {
	// misc
	var (
		auctionResponse *Auction.Response
		err             error
	)
	realmManager := Entity.RealmManager{Client: self.CacheClient}
	result := DownloadResult{Result: Result{realm: realm}}

	// fetching the auction info
	auctionResponse, err = Auction.Get(realm, self.CacheClient.ApiKey)
	if err != nil {
		result.Err = errors.New(fmt.Sprintf("Auction.Get() failed (%s)", err.Error()))
		self.DownloadOut <- result
		return
	}

	// optionally halting on empty response
	if auctionResponse == nil {
		result.responseFailed = true
		self.DownloadOut <- result
		return
	}

	file := auctionResponse.Files[0]

	// checking whether the file has already been downloaded
	result.LastModified = time.Unix(file.LastModified/1000, 0)
	if !realm.LastDownloaded.IsZero() && (realm.LastDownloaded.Equal(result.LastModified) || realm.LastDownloaded.After(result.LastModified)) {
		result.AlreadyChecked = true
		self.DownloadOut <- result
		return
	}

	// fetching the actual auction data
	if result.auctionDataResponse = AuctionData.Get(realm, file.Url); result.auctionDataResponse == nil {
		result.responseFailed = true
		self.DownloadOut <- result
		return
	}

	// flagging the realm as having been downloaded
	realm.LastDownloaded = result.LastModified
	realmManager.Persist(realm)
	result.realm = realm

	// queueing it out
	self.DownloadOut <- result
}

func (self Queue) ItemizeRealm(downloadResult DownloadResult) {
	// misc
	var err error
	realm := downloadResult.realm
	result := ItemizeResult{Result: downloadResult.Result}

	// optionally halting on error
	if downloadResult.Err != nil {
		result.Err = errors.New(fmt.Sprintf("downloadResult had an error (%s)", downloadResult.Err.Error()))
		self.ItemizeOut <- result
		return
	}

	// optionally skipping failed responses or already having been checked
	if result.responseFailed || result.AlreadyChecked {
		self.ItemizeOut <- result
		return
	}

	/*
		character handling
	*/
	characterManager := Character.Manager{Client: self.CacheClient, Realm: realm}

	// gathering existing characters
	var existingCharacters []Character.Character
	existingCharacters, err = characterManager.FindAll()
	if err != nil {
		result.Err = errors.New(fmt.Sprintf("CharacterManager.FindAll() failed (%s)", err.Error()))
		self.ItemizeOut <- result
		return
	}

	// gathering found names
	uniqueFoundNames := map[string]struct{}{}
	for _, auction := range downloadResult.auctionDataResponse.Auctions.Auctions {
		uniqueFoundNames[auction.Owner] = struct{}{}
	}

	// merging existing characters in and persisting them all
	newCharacters := downloadResult.getNewCharacters(existingCharacters)
	result.characters, err = characterManager.PersistAll(existingCharacters, newCharacters)
	if err != nil {
		result.Err = errors.New(fmt.Sprintf("CharacterManager.PersistAll() failed (%s)", err.Error()))
		self.ItemizeOut <- result
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
	// result.auctions = downloadResult.auctionDataResponse.Auctions.Auctions

	// queueing it out
	self.ItemizeOut <- result
}
