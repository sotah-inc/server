package Work

import (
	"github.com/ihsw/go-download/Entity"
)

type ItemizeResults struct {
	list []ItemizeResult
}

func (self ItemizeResults) getNewItems(existingItems []Entity.Item) (newItems []Entity.Item) {
	// gathering the blizz-item-ids for uniqueness
	existingBlizzIds := map[int64]struct{}{}
	newBlizzIds := map[int64]struct{}{}
	for _, item := range existingItems {
		existingBlizzIds[item.BlizzId] = struct{}{}
	}
	for _, result := range self.list {
		for _, blizzId := range result.blizzItemIds {
			_, ok := existingBlizzIds[blizzId]
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
