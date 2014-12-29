package Work

import (
	"github.com/ihsw/go-download/Blizzard/AuctionData"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Entity/Character"
)

type DownloadResult struct {
	AuctionDataResponse AuctionData.Response
	AlreadyChecked      bool
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

func (self DownloadResult) getCharacters(existingCharacters []Character.Character) []Character.Character {
	uniqueCharacterNames := make(map[string]int)
	const (
		notFound = 0
		found    = 1
		done     = 2
	)

	// doing a first pass to gather unique character names
	for _, auction := range self.AuctionDataResponse.Auctions.Auctions {
		uniqueCharacterNames[auction.Owner] = notFound
	}
	for _, character := range existingCharacters {
		uniqueCharacterNames[character.Name] = found
	}

	// doing a second pass to generate new ones where applicable
	characters := make([]Character.Character, len(uniqueCharacterNames))
	i := 0
	for _, auction := range self.AuctionDataResponse.Auctions.Auctions {
		name := auction.Owner
		if uniqueCharacterNames[name] == notFound {
			characters[i] = Character.Character{
				Name:  auction.Owner,
				Realm: self.Realm,
			}
			uniqueCharacterNames[name] = done

			i++
		}
	}
	for _, character := range existingCharacters {
		name := character.Name
		if uniqueCharacterNames[name] == found {
			characters[i] = character
			uniqueCharacterNames[name] = done

			i++
		}
	}

	return characters
}
