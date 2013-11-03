package Entity

import (
	"encoding/json"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Config"
	"strconv"
)

/*
	funcs
*/
func NewRegionFromConfig(configRegion Config.Region) Region {
	return Region{
		Name: configRegion.Name,
		Host: configRegion.Host,
	}
}

/*
	Region
*/
type Region struct {
	Id      int64
	Name    string
	Host    string
	Locales []Locale
}

func (self Region) Marshal() (string, error) {
	var (
		s string
	)

	v := map[string]interface{}{
		"0": self.Id,
		"1": self.Name,
		"2": self.Host,
	}
	b, err := json.Marshal(v)
	if err != nil {
		return s, err
	}

	return string(b), nil
}

/*
	RegionManager
*/
type RegionManager struct {
	Client Cache.Client
}

func (self RegionManager) Namespace() string {
	return "region"
}

func (self RegionManager) Persist(region Region) (Region, error) {
	var (
		err error
		s   string
	)
	r := self.Client.Main.Redis

	// id
	isNew := region.Id == 0
	if isNew {
		cmd := r.Incr("region_id")
		if cmd.Err() != nil {
			return region, cmd.Err()
		}
		region.Id = cmd.Val()
	}

	// data
	s, err = region.Marshal()
	if err != nil {
		return region, err
	}
	bucketKey, subKey := Cache.GetBucketKey(region.Id, self.Namespace())
	cmd := r.HSet(bucketKey, subKey, s)
	if cmd.Err() != nil {
		return region, cmd.Err()
	}

	// etc
	if isNew {
		cmd := r.RPush("region_ids", strconv.FormatInt(region.Id, 10))
		if cmd.Err() != nil {
			return region, cmd.Err()
		}
	}

	return region, nil
}

func (self RegionManager) unmarshal(v map[string]interface{}) Region {
	return Region{
		Id:   int64(v["0"].(float64)),
		Name: v["1"].(string),
		Host: v["2"].(string),
	}
}

func (self RegionManager) unmarshalAll(values []map[string]interface{}) (regions []Region) {
	regions = make([]Region, len(values))
	for i, rawRegion := range values {
		regions[i] = self.unmarshal(rawRegion)
	}
	return regions
}

func (self RegionManager) FindOneById(id int64) (Region, error) {
	v, err := self.Client.Main.FetchFromId(self, id)
	return self.unmarshal(v), err
}

func (self RegionManager) FindAll() ([]Region, error) {
	var (
		err        error
		regions    []Region
		rawRegions []map[string]interface{}
	)
	main := self.Client.Main

	// fetching ids
	ids, err := main.FetchIds("region_ids", 0, -1)
	if err != nil {
		return regions, err
	}

	// fetching the values
	rawRegions, err = main.FetchFromIds(self, ids)
	if err != nil {
		return regions, err
	}

	return self.unmarshalAll(rawRegions), nil
}
