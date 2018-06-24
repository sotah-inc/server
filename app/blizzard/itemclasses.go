package blizzard

import (
	"encoding/json"
	"fmt"

	"github.com/ihsw/sotah-server/app/util"
)

const itemClassesURLFormat = "https://%s/wow/data/item/classes"

func defaultGetItemClassesURL(regionHostname string) string {
	return fmt.Sprintf(itemClassesURLFormat, regionHostname)
}

type getItemClassesURLFunc func(string) string

func newItemClassesFromHTTP(uri string) (itemClasses, error) {
	body, err := util.Download(uri)
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

type itemClassClass int

type itemClass struct {
	Class      itemClassClass `json:"class"`
	Name       string         `json:"name"`
	SubClasses []subItemClass `json:"subclasses"`
}

type itemSubClassClass int

type subItemClass struct {
	SubClass itemSubClassClass `json:"subclass"`
	Name     string            `json:"name"`
}
