package Entity

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/ihsw/go-download/app/Cache"
)

/*
	Locale
*/
type Locale struct {
	Id        int64
	Name      string
	Fullname  string
	Shortname string
	Region    Region
}

func (self Locale) marshal() (string, error) {
	localeJson := LocaleJson{
		Id:        self.Id,
		Name:      self.Name,
		Fullname:  self.Fullname,
		Shortname: self.Shortname,
		RegionId:  self.Region.Id,
	}

	return localeJson.marshal()
}

func (self Locale) IsValid() bool { return self.Id != 0 }

/*
	LocaleJson
*/
type LocaleJson struct {
	Id        int64  `json:"0"`
	Name      string `json:"1"`
	Fullname  string `json:"2"`
	Shortname string `json:"3"`
	RegionId  int64  `json:"4"`
}

func (self LocaleJson) marshal() (string, error) {
	b, err := json.Marshal(self)
	return string(b), err
}

/*
	LocaleManager
*/
type LocaleManager struct {
	Client Cache.Client
}

func (self LocaleManager) Namespace() string { return "locale" }

func (self LocaleManager) Persist(locale Locale) (Locale, error) {
	var (
		err error
		s   string
	)
	r := self.Client.Main.Redis

	// id
	isNew := !locale.IsValid()
	if isNew {
		req := r.Incr("locale_id")
		if err = req.Err(); err != nil {
			return locale, err
		}
		locale.Id = req.Val()
	}

	// data
	s, err = locale.marshal()
	if err != nil {
		return locale, err
	}
	bucketKey, subKey := Cache.GetBucketKey(locale.Id, "locale")
	req := r.HSet(bucketKey, subKey, s)
	if err = req.Err(); err != nil {
		return locale, err
	}

	// etc
	if isNew {
		id := strconv.FormatInt(locale.Id, 10)
		cmd := r.RPush("locale_ids", id)
		if err = cmd.Err(); err != nil {
			return locale, err
		}
		cmd = r.RPush(fmt.Sprintf("region:%d:locale_ids", locale.Region.Id), id)
		if err = cmd.Err(); err != nil {
			return locale, err
		}
	}

	return locale, nil
}

func (self LocaleManager) unmarshal(v string) (locale Locale, err error) {
	if v == "" {
		return
	}

	// json
	localeJson := LocaleJson{}
	b := []byte(v)
	err = json.Unmarshal(b, &localeJson)
	if err != nil {
		return
	}

	// optionally halting
	if localeJson.RegionId == 0 {
		return
	}

	// managers
	regionManager := RegionManager{Client: self.Client}

	// sub
	var region Region
	region, err = regionManager.FindOneById(localeJson.RegionId)
	if err != nil || !region.IsValid() {
		return
	}

	// initial
	locale = Locale{
		Id:        localeJson.Id,
		Name:      localeJson.Name,
		Fullname:  localeJson.Fullname,
		Shortname: localeJson.Shortname,
		Region:    region,
	}
	return locale, nil
}

func (self LocaleManager) unmarshalAll(values []string) (locales []Locale, err error) {
	locales = make([]Locale, len(values))
	for i, v := range values {
		locales[i], err = self.unmarshal(v)
		if err != nil {
			return
		}
	}
	return
}

func (self LocaleManager) findAllInIds(ids []int64) (locales []Locale, err error) {
	var values []string
	values, err = self.Client.Main.FetchFromIds(self, ids)
	if err != nil {
		return
	}

	return self.unmarshalAll(values)
}

func (self LocaleManager) findAllInList(list string) (locales []Locale, err error) {
	var ids []int64
	ids, err = self.Client.Main.FetchIds(list, 0, -1)
	if err != nil {
		return
	}
	return self.findAllInIds(ids)
}

func (self LocaleManager) FindAll() ([]Locale, error) {
	return self.findAllInList("region_ids")
}

func (self LocaleManager) FindAllInRegion(region Region) ([]Locale, error) {
	return self.findAllInList(fmt.Sprintf("region:%d:locale_ids", region.Id))
}

func (self LocaleManager) FindOneById(id int64) (locale Locale, err error) {
	var v string
	v, err = self.Client.Main.FetchFromId(self, id)
	if err != nil {
		return locale, err
	}

	return self.unmarshal(v)
}
