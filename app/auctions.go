package app

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

const auctionsURLFormat = "https://%s.api.battle.net/wow/auction/data/%s"

func defaultGetAuctionsURL(regionName string, realmName string) string {
	return fmt.Sprintf(auctionsURLFormat, regionName, realmName)
}

type getAuctionsURLFunc func(string, string) string

func newAuctions(regionName string, realmName string, getAuctionsURL getAuctionsURLFunc) (*auctions, error) {
	resp, err := http.Get(getAuctionsURL(regionName, realmName))
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	a := &auctions{}
	if err := json.Unmarshal(body, a); err != nil {
		return nil, err
	}

	return a, nil
}

type auctions struct {
	Files []auctionFile `json:"files"`
}

type auctionFile struct {
	URL          string `json:"url"`
	LastModified int64  `json:"lastModified"`
}
