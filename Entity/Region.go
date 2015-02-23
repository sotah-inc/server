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
func regionNameKey(name string) string {
	return fmt.Sprintf("region:%s:id", Util.Md5Encode(name))
}

func NewRegionManager(client Cache.Client) RegionManager { return RegionManager{Client: client} }

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
		Id:        self.Id,
		Name:      self.Name,
		Host:      self.Host,
		Queryable: self.Queryable,
	}

	return regionJson.marshal()
}

func (self Region) IsValid() bool { return self.Id != 0 }

/*
	RegionJson
*/
type RegionJson struct {
	Id        int64  `json:"0"`
	Name      string `json:"1"`
	Host      string `json:"2"`
	Queryable bool   `json:"3"`
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

func (self RegionManager) PersistAll(configRegions []Config.Region) (regions []Region, err error) {
	m := self.Client.Main

	// reformatting
	regions = make([]Region, len(configRegions))
	for i, configRegion := range configRegions {
		regions[i] = Region{
			Name:      configRegion.Name,
			Host:      configRegion.Host,
			Queryable: configRegion.Queryable,
		}
	}

	// ids
	var ids []int64
	if ids, err = m.IncrAll("region_id", len(regions)); err != nil {
		return
	}
	for i, id := range ids {
		regions[i].Id = id
	}

	// data
	persistValues := make([]Cache.PersistValue, len(regions))
	hashedNameKeys := map[string]string{}
	newIds := make([]string, len(regions))
	for i, region := range regions {
		bucketKey, subKey := Cache.GetBucketKey(region.Id, self.Namespace())

		var s string
		if s, err = region.marshal(); err != nil {
			return
		}

		persistValues[i] = Cache.PersistValue{
			BucketKey: bucketKey,
			SubKey:    subKey,
			Value:     s,
		}

		id := strconv.FormatInt(region.Id, 10)
		hashedNameKeys[regionNameKey(region.Name)] = id
		newIds[i] = id
	}
	if err = m.PersistAll(persistValues); err != nil {
		return
	}
	if err = m.SetAll(hashedNameKeys); err != nil {
		return
	}
	if err = m.RPushAll("region_ids", newIds); err != nil {
		return
	}

	return
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
		Id:        regionJson.Id,
		Name:      regionJson.Name,
		Host:      regionJson.Host,
		Queryable: regionJson.Queryable,
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
	var v string
	if v, err = self.Client.Main.FetchFromId(self, id); err != nil {
		return
	}
	return self.unmarshal(v)
}

func (self RegionManager) FindOneByName(name string) (region Region, err error) {
	var v string
	if v, err = self.Client.Main.FetchFromKey(self, regionNameKey(name)); err != nil {
		return
	}

	return self.unmarshal(v)
}

func (self RegionManager) FindAll() (regions []Region, err error) {
	main := self.Client.Main

	// fetching ids
	var ids []int64
	if ids, err = main.FetchIds("region_ids", 0, -1); err != nil {
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

func (self RegionManager) FindByIds(ids []int64) (regions []Region, err error) {
	var values []string
	if values, err = self.Client.Main.FetchFromIds(self, ids); err != nil {
		return
	}

	return self.unmarshalAll(values)
}
