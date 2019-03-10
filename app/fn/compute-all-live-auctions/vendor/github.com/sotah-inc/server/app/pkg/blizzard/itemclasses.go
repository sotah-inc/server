package blizzard

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/sotah-inc/server/app/pkg/util"
)

const itemClassesURLFormat = "https://%s/wow/data/item/classes"

// DefaultGetItemClassesURL generates a url for fetching item-classes
func DefaultGetItemClassesURL(regionHostname string) string {
	return fmt.Sprintf(itemClassesURLFormat, regionHostname)
}

// GetItemClassesURLFunc defines the expected func signature for generating a url for fetching item-classes
type GetItemClassesURLFunc func(string) string

// NewItemClassesFromHTTP loads item-classes from the http api
func NewItemClassesFromHTTP(uri string) (ItemClasses, ResponseMeta, error) {
	resp, err := Download(uri)
	if err != nil {
		return ItemClasses{}, resp, err
	}

	if resp.Status != http.StatusOK {
		return ItemClasses{}, resp, errors.New("status was not 200")
	}

	iClasses, err := NewItemClasses(resp.Body)
	if err != nil {
		return ItemClasses{}, resp, err
	}

	return iClasses, resp, nil
}

// NewItemClassesFromFilepath loads item-classes from a json file
func NewItemClassesFromFilepath(relativeFilepath string) (ItemClasses, error) {
	body, err := util.ReadFile(relativeFilepath)
	if err != nil {
		return ItemClasses{}, err
	}

	return NewItemClasses(body)
}

// NewItemClasses parses json bytes for producing item-classes
func NewItemClasses(body []byte) (ItemClasses, error) {
	icResult := &ItemClasses{}
	if err := json.Unmarshal(body, icResult); err != nil {
		return ItemClasses{}, err
	}

	return *icResult, nil
}

// ItemClasses lists out all item-classes
type ItemClasses struct {
	Classes []ItemClass `json:"classes"`
}

// ItemClassClass should be an enum as item-class-classes are fixed
type ItemClassClass int

// ItemClass contains item-specific information and sub-item kinds
type ItemClass struct {
	Class      ItemClassClass `json:"class"`
	Name       string         `json:"name"`
	SubClasses []SubItemClass `json:"subclasses"`
}

// ItemSubClassClass is the sub-class id (cannot be enum, depends on item-class-class)
type ItemSubClassClass int

// SubItemClass contains the name and sub-class id
type SubItemClass struct {
	SubClass ItemSubClassClass `json:"subclass"`
	Name     string            `json:"name"`
}
