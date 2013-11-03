package Entity

import (
	"encoding/json"
	"fmt"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Config"
	"github.com/vmihailenco/redis/v2"
	"reflect"
	"strconv"
)

/*
	funcs
*/
func NewLocaleFromConfig(configLocale Config.Locale) Locale {
	return Locale{
		Name:      configLocale.Name,
		Fullname:  configLocale.Fullname,
		Shortname: configLocale.Shortname,
	}
}

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
	var (
		s string
	)

	v := map[string]interface{}{
		"0": self.Id,
		"1": self.Name,
		"2": self.Fullname,
		"3": self.Shortname,
		"4": self.Region.Id,
	}
	b, err := json.Marshal(v)
	if err != nil {
		return s, err
	}
	return string(b), nil
}

/*
	LocaleManager
*/
type LocaleManager struct {
	Client Cache.Client
}

func (self LocaleManager) Namespace() string {
	return "locale"
}

func (self LocaleManager) Persist(locale Locale) (Locale, error) {
	var (
		err error
		s   string
	)
	r := self.Client.Main.Redis

	// id
	isNew := locale.Id == 0
	if isNew {
		req := r.Incr("region_id")
		if req.Err() != nil {
			return locale, req.Err()
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
	if req.Err() != nil {
		return locale, req.Err()
	}

	// etc
	var cmd *redis.IntCmd
	if isNew {
		id := strconv.FormatInt(locale.Id, 10)
		cmd = r.RPush("locale_ids", id)
		if cmd.Err() != nil {
			return locale, cmd.Err()
		}
		cmd = r.RPush(fmt.Sprintf("region:%d:locale_ids", locale.Region.Id), id)
		if cmd.Err() != nil {
			return locale, cmd.Err()
		}
	}

	return locale, nil
}

func (self LocaleManager) unmarshal(v map[string]interface{}) Locale {
	return Locale{
		Id:        v["0"].(int64),
		Name:      v["1"].(string),
		Fullname:  v["2"].(string),
		Shortname: v["3"].(string),
	}
}

func (self LocaleManager) unmarshalAll(values []map[string]interface{}) (locales []Locale) {
	locales = make([]Locale, len(values))
	for i, v := range values {
		for key, value := range v {
			fmt.Println(fmt.Sprintf("%s is %s", key, reflect.TypeOf(value)))
			fmt.Println(value)
		}
		locales[i] = self.unmarshal(v)
	}
	return locales
}

func (self LocaleManager) findAllInIds(ids []int64) (locales []Locale, err error) {
	var rawLocales []map[string]interface{}
	rawLocales, err = self.Client.Main.FetchFromIds(self, ids)
	if err != nil {
		return
	}

	return self.unmarshalAll(rawLocales), nil
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
