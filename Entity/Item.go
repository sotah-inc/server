package Entity

import (
	"encoding/json"
	"github.com/ihsw/go-download/Cache"
	"strconv"
)

/*
	Item
*/
type Item struct {
	Id      int64
	BlizzId int64
}

func (self Item) marshal() (string, error) {
	itemJson := ItemJson{
		Id:      self.Id,
		BlizzId: self.BlizzId,
	}

	return itemJson.marshal()
}

/*
	ItemJson
*/
type ItemJson struct {
	Id      int64 `json:"0"`
	BlizzId int64 `json:"1"`
}

func (self ItemJson) marshal() (string, error) {
	b, err := json.Marshal(self)
	return string(b), err
}

/*
	ItemManager
*/
type ItemManager struct {
	Client Cache.Client
}

func (self ItemManager) Namespace() string { return "item" }

func (self ItemManager) PersistAll(items []Item) ([]Item, error) {
	var (
		err error
		ids []int64
		s   string
	)
	m := self.Client.Main

	// ids
	ids, err = m.IncrAll("item_id", len(items))
	if err != nil {
		return items, err
	}
	for i, id := range ids {
		items[i].Id = id
	}

	// data
	values := make([]Cache.PersistValue, len(items))
	for i, item := range items {
		s, err = item.marshal()
		bucketKey, subKey := Cache.GetBucketKey(item.Id, self.Namespace())
		values[i] = Cache.PersistValue{
			BucketKey: bucketKey,
			SubKey:    subKey,
			Value:     s,
		}
	}
	err = m.PersistAll(values)
	if err != nil {
		return items, err
	}

	// etc
	newIds := make([]string, len(items))
	newBlizzIds := make([]string, len(items))
	for i, item := range items {
		newIds[i] = strconv.FormatInt(item.Id, 10)
		newBlizzIds[i] = strconv.FormatInt(item.BlizzId, 10)
	}
	if err = m.RPushAll("item:ids", newIds); err != nil {
		return items, err
	}
	if err = m.SAddAll("item:blizz_ids", newBlizzIds); err != nil {
		return items, err
	}

	return items, nil
}

func (self ItemManager) unmarshal(v string) (item Item, err error) {
	if v == "" {
		return
	}

	// json
	var itemJson ItemJson
	b := []byte(v)
	err = json.Unmarshal(b, &itemJson)
	if err != nil {
		return
	}

	// initial
	item = Item{
		Id:      itemJson.Id,
		BlizzId: itemJson.BlizzId,
	}

	return item, nil
}

func (self ItemManager) unmarshalAll(values []string) (items []Item, err error) {
	items = make([]Item, len(values))
	for i, v := range values {
		items[i], err = self.unmarshal(v)
		if err != nil {
			return
		}
	}
	return
}

func (self ItemManager) FindAll() (items []Item, err error) {
	m := self.Client.Main

	// fetching ids
	ids, err := m.FetchIds("item:ids", 0, -1)
	if err != nil {
		return
	}

	// fetching the values
	var values []string
	values, err = m.FetchFromIds(self, ids)
	if err != nil {
		return
	}

	return self.unmarshalAll(values)
}

func (self ItemManager) GetBlizzIds() (blizzIds []int64, err error) {
	var values []string
	if values, err = self.Client.Main.SMembers("item:blizz_ids"); err != nil {
		return
	}

	blizzIds = make([]int64, len(values))
	for i, v := range values {
		var blizzId int
		if blizzId, err = strconv.Atoi(v); err != nil {
			return
		}
		blizzIds[i] = int64(blizzId)
	}
	return
}
