package app

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

const auctionInfoURLFormat = "https://%s.api.battle.net/wow/auction/data/%s"

func defaultGetAuctionInfoURL(regionName string, realmName string) string {
	return fmt.Sprintf(auctionInfoURLFormat, regionName, realmName)
}

type getAuctionInfoURLFunc func(string, string) string

func newAuctionInfo(regionName string, realmName string, getAuctionInfoURL getAuctionInfoURLFunc) (*auctionInfo, error) {
	resp, err := http.Get(getAuctionInfoURL(regionName, realmName))
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	a := &auctionInfo{}
	if err := json.Unmarshal(body, a); err != nil {
		return nil, err
	}

	return a, nil
}

type auctionInfo struct {
	Files []auctionFile `json:"files"`
}

type auctionFile struct {
	URL          string `json:"url"`
	LastModified int64  `json:"lastModified"`
}
