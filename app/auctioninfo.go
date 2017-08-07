package app

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
)

const auctionInfoURLFormat = "https://%s.api.battle.net/wow/auction/data/%s"

func defaultGetAuctionInfoURL(regionName string, realmName string) string {
	return fmt.Sprintf(auctionInfoURLFormat, regionName, realmName)
}

type getAuctionInfoURLFunc func(string, string) string

func newAuctionInfo(regionName string, realmName string, r resolver) (*auctionInfo, error) {
	resp, err := http.Get(r.getAuctionInfoURL(regionName, realmName))
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

func (a auctionInfo) getFirstAuctions(r resolver) (*auctions, error) {
	if len(a.Files) == 0 {
		return nil, errors.New("cannot fetch first auctions with blank files")
	}

	return a.Files[0].getAuctions(r)
}

type auctionFile struct {
	URL          string `json:"url"`
	LastModified int64  `json:"lastModified"`
}

func (af auctionFile) getAuctions(r resolver) (*auctions, error) {
	return newAuctions(af.URL, r)
}
