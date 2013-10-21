package Entity

import (
	"encoding/json"
	"fmt"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Config"
	"github.com/ihsw/go-download/Util"
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
		req := r.Incr("region_id")
		if req.Err() != nil {
			return region, req.Err()
		}
		region.Id = req.Val()
	}

	// data
	s, err = region.Marshal()
	if err != nil {
		return region, err
	}
	bucketKey, subKey := Cache.GetBucketKey(region.Id, "region")
	req := r.HSet(bucketKey, subKey, s)
	if req.Err() != nil {
		return region, req.Err()
	}

	// misc
	if isNew {
		req := r.RPush("region_ids", strconv.FormatInt(region.Id, 10))
		if req.Err() != nil {
			return region, req.Err()
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

func (self RegionManager) FindAll() ([]Region, error) {
	var (
		// strings []string
		err     error
		regions []Region
	)
	main := self.Client.Main

	// fetching ids
	ids, err := main.FetchIds("region_ids", 0, -1)
	if err != nil {
		return regions, err
	}
	fmt.Println(ids)
	Util.Write("Done!")

	return regions, nil
}
