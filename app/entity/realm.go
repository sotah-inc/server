package entity

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/ihsw/go-download/app/cache"
	"github.com/ihsw/go-download/app/util"
)

/*
	funcs
*/
func realmNameKey(region Region, slug string) string {
	return fmt.Sprintf("region:%d:realm:%s:id", region.Id, util.Md5Encode(slug))
}

func NewRealmManager(region Region, client cache.Client) RealmManager {
	return RealmManager{
		Region:        region,
		RegionManager: RegionManager{Client: client},
	}
}

/*
	Realm
*/
type Realm struct {
	Id             int64
	Name           string
	Slug           string
	Battlegroup    string
	Type           string
	Status         bool
	LastChecked    time.Time
	Region         Region
	Population     string
	LastDownloaded time.Time
}

func (self Realm) marshal() (string, error) {
	lastChecked := ""
	if !self.LastChecked.IsZero() {
		lastChecked = strconv.FormatInt(self.LastChecked.Unix(), 10)
	}

	lastDownloaded := ""
	if !self.LastDownloaded.IsZero() {
		lastDownloaded = strconv.FormatInt(self.LastDownloaded.Unix(), 10)
	}

	realmJson := RealmJson{
		Id:             self.Id,
		Name:           self.Name,
		Slug:           self.Slug,
		Battlegroup:    self.Battlegroup,
		Type:           self.Type,
		Status:         self.Status,
		LastChecked:    lastChecked,
		RegionId:       self.Region.Id,
		Population:     self.Population,
		LastDownloaded: lastDownloaded,
	}
	return realmJson.marshal()
}

func (self Realm) IsValid() bool { return self.Id != 0 }

func (self Realm) Dump() string {
	return fmt.Sprintf("%s-%s", self.Region.Name, self.Slug)
}

/*
	RealmJson
*/
type RealmJson struct {
	Id             int64  `json:"0"`
	Name           string `json:"1"`
	Slug           string `json:"2"`
	Battlegroup    string `json:"3"`
	Type           string `json:"4"`
	Status         bool   `json:"5"`
	LastChecked    string `json:"6"`
	RegionId       int64  `json:"7"`
	Population     string `json:"8"`
	LastDownloaded string `json:"9"`
}

func (self RealmJson) marshal() (string, error) {
	b, err := json.Marshal(self)
	return string(b), err
}

/*
	RealmManager
*/
type RealmManager struct {
	Region        Region
	RegionManager RegionManager
}

func (self RealmManager) Namespace() string { return "realm" }

func (self RealmManager) Client() cache.Client { return self.RegionManager.Client }

func (self RealmManager) Persist(v Realm) (realm Realm, err error) {
	var realms []Realm
	realms, err = self.PersistAll([]Realm{v})
	if err != nil {
		return
	}

	return realms[0], nil
}

func (self RealmManager) PersistAll(values []Realm) (realms []Realm, err error) {
	m := self.Client().Main
	realms = values

	newValueIndexes := make(map[int]int)
	i := 0
	for j, value := range values {
		if !value.IsValid() {
			newValueIndexes[i] = j
			i++
		}
	}

	// ids
	var ids []int64
	if ids, err = m.IncrAll("realm_id", len(newValueIndexes)); err != nil {
		return
	}
	for i, id := range ids {
		realms[newValueIndexes[i]].Id = id
	}

	// data
	persistValues := make([]cache.PersistValue, len(realms))
	newIds := make([]string, len(newValueIndexes))
	hashedNameKeys := map[string]string{}
	for i, realm := range realms {
		bucketKey, subKey := cache.GetBucketKey(realm.Id, self.Namespace())

		var s string
		if s, err = realm.marshal(); err != nil {
			return
		}

		persistValues[i] = cache.PersistValue{
			BucketKey: bucketKey,
			SubKey:    subKey,
			Value:     s,
		}
		_, isNew := newValueIndexes[i]
		if isNew {
			id := strconv.FormatInt(realm.Id, 10)
			newIds[i] = id
			hashedNameKeys[realmNameKey(realm.Region, realm.Slug)] = id
		}
	}
	if err = m.PersistAll(persistValues); err != nil {
		return
	}
	if err = m.RPushAll(fmt.Sprintf("region:%d:realm_ids", self.Region.Id), newIds); err != nil {
		return
	}
	if err = m.SetAll(hashedNameKeys); err != nil {
		return
	}

	return
}

func (self RealmManager) unmarshal(v string) (realm Realm, err error) {
	var realms []Realm
	if realms, err = self.unmarshalAll([]string{v}); err != nil {
		return
	}

	if len(realms) == 0 {
		return
	}

	return realms[0], nil
}

func (self RealmManager) unmarshalAll(values []string) (realms []Realm, err error) {
	// misc
	realms = make([]Realm, len(values))

	// resolving the realms
	regionIds := []int64{}
	regionKeysBack := map[int64][]int64{}
	for i, v := range values {
		if len(v) == 0 {
			continue
		}

		// json
		realmJson := RealmJson{}
		if err = json.Unmarshal([]byte(v), &realmJson); err != nil {
			return
		}

		// initial
		realm := Realm{
			Id:          realmJson.Id,
			Name:        realmJson.Name,
			Slug:        realmJson.Slug,
			Battlegroup: realmJson.Battlegroup,
			Type:        realmJson.Type,
			Status:      realmJson.Status,
			Population:  realmJson.Population,
		}

		// last-downloaded and last-checked
		if len(realmJson.LastDownloaded) > 0 {
			var lastDownloaded int64
			if lastDownloaded, err = strconv.ParseInt(realmJson.LastDownloaded, 10, 64); err != nil {
				return
			}

			realm.LastDownloaded = time.Unix(lastDownloaded, 0)
		}
		if len(realmJson.LastChecked) > 0 {
			var lastChecked int64
			if lastChecked, err = strconv.ParseInt(realmJson.LastChecked, 10, 64); err != nil {
				return
			}

			realm.LastChecked = time.Unix(lastChecked, 0)
		}

		realms[i] = realm

		// region
		regionId := realmJson.RegionId
		if _, ok := regionKeysBack[regionId]; !ok {
			regionKeysBack[regionId] = []int64{int64(i)}
			regionIds = append(regionIds, regionId)
		} else {
			regionKeysBack[regionId] = append(regionKeysBack[regionId], int64(i))
		}
	}

	// resolving the regions
	var regions []Region
	if regions, err = self.RegionManager.FindByIds(regionIds); err != nil {
		return
	}
	for _, region := range regions {
		if !region.IsValid() {
			err = errors.New("Invalid region found!")
			return
		}
		for _, i := range regionKeysBack[region.Id] {
			realms[i].Region = region
		}
	}

	return
}

func (self RealmManager) FindAll() (realms []Realm, err error) {
	main := self.Client().Main

	// fetching ids
	ids, err := main.FetchIds(fmt.Sprintf("region:%d:realm_ids", self.Region.Id), 0, -1)
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

func (self RealmManager) FindByIds(ids []int64) (realms []Realm, err error) {
	var values []string
	if values, err = self.Client().Main.FetchFromIds(self, ids); err != nil {
		return
	}

	return self.unmarshalAll(values)
}

func (self RealmManager) FindOneById(id int64) (realm Realm, err error) {
	v, err := self.Client().Main.FetchFromId(self, id)
	if err != nil {
		return
	}

	return self.unmarshal(v)
}

func (self RealmManager) FindOneBySlug(slug string) (realm Realm, err error) {
	var v string
	v, err = self.Client().Main.FetchFromKey(self, realmNameKey(self.Region, slug))
	if err != nil {
		return
	}

	return self.unmarshal(v)
}
