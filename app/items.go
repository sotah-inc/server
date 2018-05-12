package main

import (
	"encoding/json"
	"fmt"

	"github.com/ihsw/sotah-server/app/util"
)

const itemURLFormat = "https://%s/wow/item/%d"

func defaultGetItemURL(regionHostname string, ID itemID) string {
	return fmt.Sprintf(itemURLFormat, regionHostname, ID)
}

type getItemURLFunc func(string, itemID) string

func newItemFromHTTP(reg region, ID itemID, r resolver) (*item, error) {
	body, err := r.get(r.getItemURL(reg.Hostname, ID))
	if err != nil {
		return nil, err
	}

	return newItem(body)
}

func newItemFromFilepath(relativeFilepath string) (*item, error) {
	body, err := util.ReadFile(relativeFilepath)
	if err != nil {
		return nil, err
	}

	return newItem(body)
}

func newItem(body []byte) (*item, error) {
	i := &item{}
	if err := json.Unmarshal(body, i); err != nil {
		return nil, err
	}

	return i, nil
}

type itemID int64

type item struct {
	ID   itemID `json:"id"`
	Name string `json:"name"`
}

type items struct {
	Items itemsList `json:"items"`
}

type itemsList []item
