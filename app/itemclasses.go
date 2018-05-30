package main

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ihsw/sotah-server/app/util"
)

const itemClassesURLFormat = "https://%s/wow/data/item/classes"

func defaultGetItemClassesURL(regionHostname string) string {
	return fmt.Sprintf(itemClassesURLFormat, regionHostname)
}

type getItemClassesURLFunc func(string) string

func newItemClassesResultFromHTTP(r resolver) (itemClassesResult, error) {
	if r.config == nil {
		return itemClassesResult{}, errors.New("Config cannot be nil")
	}

	primaryRegion, err := r.config.Regions.getPrimaryRegion()
	if err != nil {
		return itemClassesResult{}, err
	}

	body, err := r.get(r.getItemClassesURL(primaryRegion.Hostname))
	if err != nil {
		return itemClassesResult{}, err
	}

	return newItemClassesResult(body)
}

func newItemClassesResultFromFilepath(relativeFilepath string) (itemClassesResult, error) {
	body, err := util.ReadFile(relativeFilepath)
	if err != nil {
		return itemClassesResult{}, err
	}

	return newItemClassesResult(body)
}

func newItemClassesResult(body []byte) (itemClassesResult, error) {
	icResult := &itemClassesResult{}
	if err := json.Unmarshal(body, icResult); err != nil {
		return itemClassesResult{}, err
	}

	return *icResult, nil
}

type itemClassesResult struct {
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
