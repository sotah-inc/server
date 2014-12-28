package Work

import (
	"github.com/ihsw/go-download/Blizzard/AuctionData"
	"github.com/ihsw/go-download/Entity"
)

type DownloadResult struct {
	AuctionDataResponse AuctionData.Response
	Error               error
	Realm               Entity.Realm
}

func (self DownloadResult) getBlizzItemIds() []int64 {
	// gather unique blizz item ids
	uniqueBlizzItemIds := make(map[int64]struct{})
	for _, auction := range self.AuctionDataResponse.Auctions.Auctions {
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
	for _, auction := range self.AuctionDataResponse.Auctions.Auctions {
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
