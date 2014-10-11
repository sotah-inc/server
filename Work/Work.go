package Work

import (
	"github.com/ihsw/go-download/Blizzard/Auction"
	"github.com/ihsw/go-download/Blizzard/AuctionData"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Entity"
	"time"
)

/*
	DownloadResult
*/
type DownloadResult struct {
	AuctionDataResponse AuctionData.Response
	Error               error
	Realm               Entity.Realm
}

func (self DownloadResult) getBlizzItemIds() []int64 {
	// gather unique blizz item ids
	uniqueBlizzItemIds := make(map[int64]struct{})
	for _, auction := range self.AuctionDataResponse.GetAuctions() {
		blizzItemId := auction.Item
		_, valid := uniqueBlizzItemIds[blizzItemId]
		if !valid {
			uniqueBlizzItemIds[blizzItemId] = struct{}{}
		}
	}

	// formatting
	blizzItemIds := make([]int64, len(uniqueBlizzItemIds))
	i := 0
	for blizzItemId, _ := range uniqueBlizzItemIds {
		blizzItemIds[i] = blizzItemId
		i++
	}

	return blizzItemIds
}

func (self DownloadResult) getCharacters() []Entity.Character {
	// gathering unique character names
	uniqueCharacterNames := make(map[string]struct{})
	for _, auction := range self.AuctionDataResponse.GetAuctions() {
		name := auction.Owner
		_, valid := uniqueCharacterNames[name]
		if !valid {
			uniqueCharacterNames[name] = struct{}{}
		}
	}

	// formatting
	characters := make([]Entity.Character, len(uniqueCharacterNames))
	i := 0
	for name, _ := range uniqueCharacterNames {
		characters[i] = Entity.Character{
			Name:  name,
			Realm: self.Realm,
		}
		i++
	}

	return characters
}

/*
	ItemizeResult
*/
type ItemizeResult struct {
	Error        error
	Realm        Entity.Realm
	BlizzItemIds []int64
	Characters   []Entity.Character
	Auctions     []AuctionData.Auction
}

/*
	ItemizeResults
*/
type ItemizeResults struct {
	List []ItemizeResult
}

func (self ItemizeResults) GetUniqueItems() (items []Entity.Item) {
	blizzItemIds := make(map[int64]struct{})
	for _, result := range self.List {
		for _, blizzItemId := range result.BlizzItemIds {
			_, valid := blizzItemIds[blizzItemId]
			if !valid {
				blizzItemIds[blizzItemId] = struct{}{}
			}
		}
	}

	items = make([]Entity.Item, len(blizzItemIds))
	i := 0
	for blizzItemId, _ := range blizzItemIds {
		items[i] = Entity.Item{BlizzId: blizzItemId}
		i++
	}

	return items
}

/*
	funcs
*/
func DownloadRealm(realm Entity.Realm, cacheClient Cache.Client, out chan DownloadResult) {
	// misc
	var auctionResponse Auction.Response
	realmManager := Entity.RealmManager{Client: cacheClient}
	result := DownloadResult{Realm: realm}

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
	result := ItemizeResult{Realm: realm}

	// optionally halting on error
	if downloadResult.Error != nil {
		result.Error = downloadResult.Error
		out <- result
		return
	}

	// gathering blizz-item-ids for post-itemize processing
	result.BlizzItemIds = downloadResult.getBlizzItemIds()

	// gathering characters and persisting them
	characters := downloadResult.getCharacters()
	characterManager := Entity.CharacterManager{Client: cacheClient}
	result.Characters, err = characterManager.PersistAll(realm, characters)
	if err != nil {
		result.Error = err
		out <- result
		return
	}

	// gathering auctions for post-itemize processing
	result.Auctions = downloadResult.AuctionDataResponse.GetAuctions()

	// queueing it out
	out <- result
}
