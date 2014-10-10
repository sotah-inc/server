package Entity

import (
	"encoding/json"
	"fmt"
	"github.com/ihsw/go-download/Cache"
	"strconv"
)

/*
	Region
*/
type Region struct {
	Id        int64
	Name      string
	Host      string
	Queryable bool
}

func (self Region) marshal() (string, error) {
	regionJson := RegionJson{
		Id:   self.Id,
		Name: self.Name,
		Host: self.Host,
	}

	return regionJson.marshal()
}

func (self Region) IsValid() bool { return self.Id != 0 }

/*
	RegionJson
*/
type RegionJson struct {
	Id   int64  `json:"0"`
	Name string `json:"1"`
	Host string `json:"2"`
}

func (self RegionJson) marshal() (string, error) {
	b, err := json.Marshal(self)
	return string(b), err
}

/*
	RegionManager
*/
type RegionManager struct {
	Client Cache.Client
}

func (self RegionManager) Namespace() string { return "region" }

func (self RegionManager) Persist(region Region) (Region, error) {
	var (
		err error
		s   string
	)
	w := self.Client.Main
	r := w.Redis

	// id
	isNew := !region.IsValid()
	if isNew {
		cmd := r.Incr("region_id")
		if err = cmd.Err(); err != nil {
			return region, err
		}
		region.Id = cmd.Val()
	}

	// data
	s, err = region.marshal()
	if err != nil {
		return region, err
	}
	bucketKey, subKey := Cache.GetBucketKey(region.Id, self.Namespace())
	err = w.Persist(bucketKey, subKey, s)
	if err != nil {
		return region, err
	}

	// etc
	if isNew {
		cmd := r.RPush("region_ids", strconv.FormatInt(region.Id, 10))
		if err = cmd.Err(); err != nil {
			return region, err
		}
	}

	return region, nil
}

func (self RegionManager) unmarshal(v string) (region Region, err error) {
	if v == "" {
		return
	}

	// json
	var regionJson RegionJson
	b := []byte(v)
	err = json.Unmarshal(b, &regionJson)
	if err != nil {
		return
	}

	// initial
	region = Region{
		Id:   regionJson.Id,
		Name: regionJson.Name,
		Host: regionJson.Host,
	}
	return region, nil
}

func (self RegionManager) unmarshalAll(values []string) (regions []Region, err error) {
	regions = make([]Region, len(values))
	for i, v := range values {
		regions[i], err = self.unmarshal(v)
		if err != nil {
			return
		}
	}
	return
}

func (self RegionManager) FindOneById(id int64) (region Region, err error) {
	v, err := self.Client.Main.FetchFromId(self, id)
	if err != nil {
		return
	}
	return self.unmarshal(v)
}

func (self RegionManager) FindAll() (regions []Region, err error) {
	main := self.Client.Main

	// fetching ids
	ids, err := main.FetchIds("region_ids", 0, -1)
	if err != nil {
		return
	}

	// fetching the values
	var values []string
	values, err = main.FetchFromIds(self, ids)
	if err != nil {
		return
	}

	return self.unmarshalAll(values)
}
