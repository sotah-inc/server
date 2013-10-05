package Entity

import (
	"encoding/json"
	"fmt"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Config"
)

/*
	funcs
*/
func UnmarshalRegion(v map[string]interface{}) Region {
	return Region{
		Id:   v["0"].(int64),
		Name: v["1"].(string),
		Host: v["2"].(string),
	}
}

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

func (self RegionManager) Persist(region Region) (Region, error) {
	// misc
	var (
		err error
		s   string
	)
	wrapper := self.Client.Main

	if region.Id == 0 {
		region.Id, err = wrapper.Incr("region_id")
		if err != nil {
			return region, err
		}
	}

	s, err = region.Marshal()
	if err != nil {
		return region, err
	}

	fmt.Println(Cache.GetBucketKey(region.Id, "region"))
	fmt.Println(s)

	return region, nil
}
