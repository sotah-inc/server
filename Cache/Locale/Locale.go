package Locale

import (
	"encoding/json"
	"fmt"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Config"
)

// structs and consts
type Locale struct {
	Id        int64
	Name      string
	Fullname  string
	Shortname string
}

// functions
func New(locale Config.Locale) Locale {
	l := Locale{
		Name:      locale.Name,
		Fullname:  locale.Fullname,
		Shortname: locale.Shortname,
	}
	return l
}

func NewFromList(locales []Config.Locale) []Locale {
	var l []Locale
	for _, locale := range locales {
		l = append(l, New(locale))
	}
	return l
}

func Unmarshal(v map[string]interface{}) Locale {
	return Locale{
		Name:      v["0"].(string),
		Fullname:  v["1"].(string),
		Shortname: v["2"].(string),
	}
}

// methods
func (self Locale) Marshal() (string, error) {
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

func (self Locale) Persist(cache Cache.Cache) (Locale, error) {
	var (
		err error
		s   string
	)

	// persisting
	redis := cache.Main
	if self.Id == 0 {
		self.Id, err = Cache.Incr("locale_id", redis)
		if err != nil {
			return self, err
		}

		s, err = self.Marshal()
		if err != nil {
			return self, err
		}
		fmt.Println(s)
	} else {

	}

	return self, nil
}
