package Entity

import (
	"encoding/json"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Config"
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

func (self Locale) Marshal() (string, error) {
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

func (self LocaleManager) Persist(locale Locale) (Locale, error) {
	var (
		err error
		s   string
	)
	main := self.Client.Main

	if locale.Id == 0 {
		locale.Id, err = main.Incr("region_id")
		if err != nil {
			return locale, err
		}
	}

	s, err = locale.Marshal()
	if err != nil {
		return locale, err
	}

	bucketKey, subKey := Cache.GetBucketKey(locale.Id, "locale")
	err = main.HSet(bucketKey, subKey, s)
	if err != nil {
		return locale, err
	}

	return locale, nil
}

func (self LocaleManager) Unmarshal(v map[string]interface{}) Locale {
	return Locale{
		Id:        v["0"].(int64),
		Name:      v["1"].(string),
		Fullname:  v["2"].(string),
		Shortname: v["3"].(string),
	}
}
