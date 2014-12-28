package Work

import (
	"github.com/ihsw/go-download/Entity"
)

type ItemizeResults struct {
	List []ItemizeResult
}

func (self ItemizeResults) GetUniqueItems() (items []Entity.Item) {
	blizzItemIds := make(map[int64]struct{})
	for _, result := range self.List {
		if result.AlreadyChecked {
			continue
		}

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
