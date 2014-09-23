package Entity

import (
	"encoding/json"
	"fmt"
	"github.com/ihsw/go-download/Cache"
	redis "github.com/vmihailenco/redis/v2"
	"strconv"
	"time"
)

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
		lastDownloaded = strconv.FormatInt(self.LastChecked.Unix(), 10)
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
	Client Cache.Client
}

func (self RealmManager) Namespace() string { return "realm" }

func (self RealmManager) Persist(realm Realm) (Realm, error) {
	var (
		err error
		s   string
	)
	r := self.Client.Main.Redis

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
	bucketKey, subKey := Cache.GetBucketKey(realm.Id, "realm")
	req := r.HSet(bucketKey, subKey, s)
	if err = req.Err(); err != nil {
		return realm, err
	}

	// etc
	var cmd *redis.IntCmd
	if isNew {
		id := strconv.FormatInt(realm.Id, 10)
		cmd = r.RPush("realm_ids", id)
		if err = cmd.Err(); err != nil {
			return realm, err
		}
		cmd = r.RPush(fmt.Sprintf("region:%d:realm_ids", realm.Region.Id), id)
		if err = cmd.Err(); err != nil {
			return realm, err
		}
	}

	return realm, nil
}

func (self RealmManager) unmarshal(v string) (realm Realm, err error) {
	if v == "" {
		return
	}

	// json
	var realmJson RealmJson
	b := []byte(v)
	err = json.Unmarshal(b, &realmJson)
	if err != nil {
		return
	}

	// fmt.Println(fmt.Sprintf("lastChecked: %s", realmJson.LastChecked))
	// fmt.Println(fmt.Sprintf("LastDownloaded: %s", realmJson.LastDownloaded))

	// initial
	realm = Realm{
		Id:          realmJson.Id,
		Name:        realmJson.Name,
		Slug:        realmJson.Slug,
		Battlegroup: realmJson.Battlegroup,
		Type:        realmJson.Type,
		Status:      realmJson.Status,
		Population:  realmJson.Population,
	}
	return realm, nil
}

func (self RealmManager) unmarshalAll(values []string) (realms []Realm, err error) {
	realms = make([]Realm, len(values))
	for i, v := range values {
		realms[i], err = self.unmarshal(v)
		if err != nil {
			return
		}
	}
	return
}

func (self RealmManager) FindByRegion(region Region) (realms []Realm, err error) {
	main := self.Client.Main

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
