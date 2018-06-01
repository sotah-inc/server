package main

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
	"github.com/ihsw/sotah-server/app/util"
)

const itemClassesURLFormat = "https://%s/wow/data/item/classes"

func defaultGetItemClassesURL(regionHostname string) string {
	return fmt.Sprintf(itemClassesURLFormat, regionHostname)
}

type getItemClassesURLFunc func(string) string

func newItemClassesFromMessenger(mess messenger) (itemClasses, error) {
	msg, err := mess.request(subjects.ItemClasses, []byte{})
	if err != nil {
		return itemClasses{}, err
	}

	if msg.Code != codes.Ok {
		return itemClasses{}, errors.New(msg.Err)
	}

	iClasses := itemClasses{}
	if err := json.Unmarshal([]byte(msg.Data), &iClasses); err != nil {
		return itemClasses{}, err
	}

	return iClasses, nil
}

func newItemClassesFromHTTP(r resolver) (itemClasses, error) {
	if r.config == nil {
		return itemClasses{}, errors.New("Config cannot be nil")
	}

	primaryRegion, err := r.config.Regions.getPrimaryRegion()
	if err != nil {
		return itemClasses{}, err
	}

	body, err := r.get(r.getItemClassesURL(primaryRegion.Hostname))
	if err != nil {
		return itemClasses{}, err
	}

	return newItemClasses(body)
}

func newItemClassesFromFilepath(relativeFilepath string) (itemClasses, error) {
	body, err := util.ReadFile(relativeFilepath)
	if err != nil {
		return itemClasses{}, err
	}

	return newItemClasses(body)
}

func newItemClasses(body []byte) (itemClasses, error) {
	icResult := &itemClasses{}
	if err := json.Unmarshal(body, icResult); err != nil {
		return itemClasses{}, err
	}

	return *icResult, nil
}

type itemClasses struct {
	Classes []itemClass `json:"classes"`
}

type itemClass struct {
	Class      int            `json:"class"`
	Name       string         `json:"name"`
	SubClasses []subItemClass `json:"subclasses"`
}

type subItemClass struct {
	SubClass int    `json:"subclass"`
	Name     string `json:"name"`
}
