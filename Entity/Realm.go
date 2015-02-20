package Entity

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Util"
	"strconv"
	"time"
)

/*
	funcs
*/
func realmNameKey(region Region, slug string) string {
	return fmt.Sprintf("region:%d:realm:%s:id", region.Id, Util.Md5Encode(slug))
}

func NewRealmManager(client Cache.Client) RealmManager {
	return RealmManager{
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
	RegionManager RegionManager
}

func (self RealmManager) Namespace() string { return "realm" }

func (self RealmManager) Client() Cache.Client { return self.RegionManager.Client }

func (self RealmManager) Persist(realm Realm) (Realm, error) {
	var (
		err error
		s   string
	)
	w := self.Client().Main
	r := w.Redis

	// id
	isNew := !realm.IsValid()
	if isNew {
		req := r.Incr("realm_id")
		if err = req.Err(); err != nil {
			return realm, err
		}
		realm.Id = req.Val()
	}

	// data
	s, err = realm.marshal()
	if err != nil {
		return realm, err
	}
	bucketKey, subKey := Cache.GetBucketKey(realm.Id, self.Namespace())
	err = w.Persist(bucketKey, subKey, s)
	if err != nil {
		return realm, err
	}

	// etc
	if isNew {
		id := strconv.FormatInt(realm.Id, 10)
		cmd := r.RPush("realm_ids", id)
		if err = cmd.Err(); err != nil {
			return realm, err
		}
		cmd = r.RPush(fmt.Sprintf("region:%d:realm_ids", realm.Region.Id), id)
		if err = cmd.Err(); err != nil {
			return realm, err
		}
		setCmd := r.Set(realmNameKey(realm.Region, realm.Slug), id)
		if err = setCmd.Err(); err != nil {
			return realm, err
		}
	}

	return realm, nil
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

func (self RealmManager) FindByRegion(region Region) (realms []Realm, err error) {
	main := self.Client().Main

	// fetching ids
	ids, err := main.FetchIds(fmt.Sprintf("region:%d:realm_ids", region.Id), 0, -1)
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

func (self RealmManager) FindOneByRegionAndSlug(region Region, slug string) (realm Realm, err error) {
	var v string
	v, err = self.Client().Main.FetchFromKey(self, realmNameKey(region, slug))
	if err != nil {
		return
	}

	return self.unmarshal(v)
}
