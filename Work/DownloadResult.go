package Work

import (
	"github.com/ihsw/go-download/Blizzard/AuctionData"
	"github.com/ihsw/go-download/Entity/Character"
)

type DownloadResult struct {
	Result
	auctionDataResponse *AuctionData.Response
}

func (self DownloadResult) getBlizzItemIds() []int64 {
	// gather unique blizz item ids
	uniqueBlizzItemIds := make(map[int64]struct{})
	for _, auction := range self.auctionDataResponse.Auctions.Auctions {
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

func (self DownloadResult) getNewCharacters(existingCharacters []Character.Character) (newCharacters []Character.Character) {
	// misc
	auctions := self.auctionDataResponse.Auctions.Auctions

	// gathering the names for uniqueness
	existingNames := make(map[string]struct{})
	newNames := make(map[string]struct{})
	for _, character := range existingCharacters {
		existingNames[character.Name] = struct{}{}
	}
	for _, auction := range auctions {
		name := auction.Owner
		_, ok := existingNames[name]
		if ok {
			continue
		}

		newNames[name] = struct{}{}
	}

	// doing a second pass to fill new ones in
	newCharacters = make([]Character.Character, len(newNames))
	i := 0
	for name, _ := range newNames {
		newCharacters[i] = Character.Character{
			Name:  name,
			Realm: self.realm,
		}
		i++
	}

	return newCharacters
}
