package Locale

import (
	"encoding/json"
	"fmt"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Cache/Region"
	"github.com/ihsw/go-download/Config"
)

// structs and consts
type Data struct {
	Id        int64
	Name      string
	Fullname  string
	Shortname string
	Region    Region.Data
}

type Manager struct {
	Client Cache.Client
}

// functions
func New(locale Config.Locale) Data {
	l := Data{
		Name:      locale.Name,
		Fullname:  locale.Fullname,
		Shortname: locale.Shortname,
	}
	return l
}

func NewFromList(locales []Config.Locale) map[int64]Data {
	var l map[int64]Data
	for _, locale := range locales {
		l = append(l, New(locale))
	}
	return l
}

func Unmarshal(v map[string]interface{}) Data {
	return Data{
		Name:      v["0"].(string),
		Fullname:  v["1"].(string),
		Shortname: v["2"].(string),
	}
}

// methods
func (self Data) Marshal() (string, error) {
	var (
		s string
	)

	v := map[string]interface{}{
		"0": self.Id,
		"1": self.Name,
		"2": self.Fullname,
		"3": self.Shortname,
	}
	b, err := json.Marshal(v)
	if err != nil {
		return s, err
	}
	return string(b), nil
}

func (self Manager) Persist(locale Data) (Data, error) {
	var (
		err error
		s   string
	)

	// persisting
	redis := self.Client.Main
	if locale.Id == 0 {
		locale.Id, err = Cache.Incr("locale_id", redis)
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
