package Entity

import (
	"encoding/json"
	"fmt"
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
	Locales map[int64]Locale
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
	main := self.Client.Main

	// id
	isNew := region.Id == 0
	if isNew {
		region.Id, err = main.Incr("region_id")
		if err != nil {
			return region, err
		}
	}

	// data
	s, err = region.Marshal()
	if err != nil {
		return region, err
	}
	bucketKey, subKey := Cache.GetBucketKey(region.Id, "region")
	err = main.HSet(bucketKey, subKey, s)
	if err != nil {
		return region, err
	}

	// misc
	if isNew {
		err = main.RPush("region_ids", strconv.FormatInt(region.Id, 10))
		if err != nil {
			return region, err
		}
	}

	return region, nil
}

func (self RegionManager) Unmarshal(v map[string]interface{}) Region {
	return Region{
		Id:   int64(v["0"].(float64)),
		Name: v["1"].(string),
		Host: v["2"].(string),
	}
}

func (self RegionManager) FindOneById(id int64) (Region, error) {
	v, err := self.Client.Main.FetchFromId(self, id)
	return self.Unmarshal(v), err
}

func (self RegionManager) FindAll() {
	main := self.Client.Main
	values, err := main.LRange("region_ids", 0, -1)
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println(values)
}
