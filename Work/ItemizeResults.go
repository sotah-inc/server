package Work

import (
	"github.com/ihsw/go-download/Entity"
)

type ItemizeResults struct {
	list []ItemizeResult
}

func (self ItemizeResults) getNewItems(existingBlizzIds []int64) (newItems []Entity.Item) {
	// gathering the blizz-ids for uniqueness
	existingBlizzIdFlags := map[int64]struct{}{}
	for _, blizzId := range existingBlizzIds {
		existingBlizzIdFlags[blizzId] = struct{}{}
	}

	newBlizzIds := map[int64]struct{}{}
	for _, result := range self.list {
		for _, blizzId := range result.blizzItemIds {
			_, ok := existingBlizzIdFlags[blizzId]
			if ok {
				continue
			}

			newBlizzIds[blizzId] = struct{}{}
		}
	}

	newItems = []Entity.Item{}
	i := 0
	for blizzId, _ := range newBlizzIds {
		newItems = append(newItems, Entity.Item{BlizzId: blizzId})
		i++
	}

	return newItems
}
