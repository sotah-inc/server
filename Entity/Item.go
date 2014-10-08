package Entity

import (
	"encoding/json"
	"fmt"
	"github.com/ihsw/go-download/Cache"
	"strconv"
)

/*
	Item
*/
type Item struct {
	Id      int64
	BlizzId uint64
}

func (self Item) Marshal() (string, error) {
	itemJson := ItemJson{
		Id:      self.Id,
		BlizzId: self.BlizzId,
	}

	return itemJson.marshal()
}

func (self Item) String() string {
	return fmt.Sprintf("Item[Id: %d, BlizzId: %d]",
		self.Id,
		self.BlizzId,
	)
}

func (self Item) IsValid() bool { return self.Id != 0 }

/*
	ItemJson
*/
type ItemJson struct {
	Id      int64  `json:"0"`
	BlizzId uint64 `json:"1"`
}

func (self ItemJson) marshal() (string, error) {
	b, err := json.Marshal(self)
	return string(b), err
}

func (self ItemJson) String() string {
	return fmt.Sprintf("ItemJson[Id: %d, BlizzId: %d]",
		self.Id,
		self.BlizzId,
	)
}

/*
	ItemManager
*/
type ItemManager struct {
	Client Cache.Client
}

func (self ItemManager) Namespace() string { return "item" }

func (self ItemManager) Persist(item Item) (Item, error) {
	var (
		err error
		s   string
	)
	w := self.Client.Main
	r := w.Redis

	// id
	isNew := item.Id == 0
	if isNew {
		cmd := r.Incr("item_id")
		if err = cmd.Err(); err != nil {
			return item, err
		}
		item.Id = cmd.Val()
	}

	// data
	s, err = item.Marshal()
	if err != nil {
		return item, err
	}
	bucketKey, subKey := Cache.GetBucketKey(item.Id, self.Namespace())
	err = w.Persist(bucketKey, subKey, s)
	if err != nil {
		return item, err
	}

	// etc
	if isNew {
		cmd := r.RPush("item_ids", strconv.FormatInt(item.Id, 10))
		if err = cmd.Err(); err != nil {
			return item, err
		}
	}

	return item, nil
}

// func (self RegionManager) unmarshal(v string) (region Region, err error) {
// 	if v == "" {
// 		return
// 	}

// 	// json
// 	var regionJson RegionJson
// 	b := []byte(v)
// 	err = json.Unmarshal(b, &regionJson)
// 	if err != nil {
// 		return
// 	}

// 	// initial
// 	region = Region{
// 		Id:   regionJson.Id,
// 		Name: regionJson.Name,
// 		Host: regionJson.Host,
// 	}
// 	return region, nil
// }

// func (self RegionManager) unmarshalAll(values []string) (regions []Region, err error) {
// 	regions = make([]Region, len(values))
// 	for i, v := range values {
// 		regions[i], err = self.unmarshal(v)
// 		if err != nil {
// 			return
// 		}
// 	}
// 	return
// }

// func (self RegionManager) FindOneById(id int64) (region Region, err error) {
// 	v, err := self.Client.Main.FetchFromId(self, id)
// 	if err != nil {
// 		return
// 	}
// 	return self.unmarshal(v)
// }

// func (self RegionManager) FindAll() (regions []Region, err error) {
// 	main := self.Client.Main

// 	// fetching ids
// 	ids, err := main.FetchIds("region_ids", 0, -1)
// 	if err != nil {
// 		return
// 	}

// 	// fetching the values
// 	var values []string
// 	values, err = main.FetchFromIds(self, ids)
// 	if err != nil {
// 		return
// 	}

// 	return self.unmarshalAll(values)
// }
