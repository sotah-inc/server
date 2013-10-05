package Entity

import (
	"encoding/json"
	"fmt"
	"github.com/ihsw/go-download/Cache"
)

/*
	funcs
*/
func UnmarshalLocale(v map[string]interface{}) Locale {
	return Locale{
		Id:        v["0"].(int64),
		Name:      v["1"].(string),
		Fullname:  v["2"].(string),
		Shortname: v["3"].(string),
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

	// persisting
	wrapper := self.Client.Main
	if locale.Id == 0 {
		locale.Id, err = wrapper.Incr("locale_id")
		if err != nil {
			return locale, err
		}

		s, err = locale.Marshal()
		if err != nil {
			return locale, err
		}
		fmt.Println(s)
	} else {

	}

	return locale, nil
}
