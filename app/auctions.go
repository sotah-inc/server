package main

import (
	"encoding/json"
	"errors"

	"github.com/ihsw/sotah-server/app/codes"

	"github.com/ihsw/sotah-server/app/subjects"

	"github.com/ihsw/sotah-server/app/util"
)

func defaultGetAuctionsURL(url string) string {
	return url
}

type getAuctionsURLFunc func(url string) string

func newAuctionsFromHTTP(url string, r resolver) (*auctions, error) {
	body, err := r.get(r.getAuctionsURL(url))
	if err != nil {
		return nil, err
	}

	return newAuctions(body)
}

func newAuctionsFromFilepath(relativeFilepath string) (*auctions, error) {
	body, err := util.ReadFile("./TestData/auctions.json")
	if err != nil {
		return nil, err
	}

	return newAuctions(body)
}

func newAuctionsFromMessenger(rea *realm, mess messenger) (*auctions, error) {
	am := listenForAuctionsMessage{
		RegionName: rea.region.Name,
		RealmSlug:  rea.Slug,
	}
	encodedMessage, err := json.Marshal(am)
	if err != nil {
		return nil, err
	}

	msg, err := mess.request(subjects.Auctions, encodedMessage)
	if err != nil {
		return nil, err
	}

	if msg.Code != codes.Ok {
		return nil, errors.New(msg.Err)
	}

	return newAuctions([]byte(msg.Data))
}

func newAuctions(body []byte) (*auctions, error) {
	a := &auctions{}
	if err := json.Unmarshal(body, a); err != nil {
		return nil, err
	}

	return a, nil
}

type auctions struct {
	Realms   []auctionRealm `json:"realms"`
	Auctions []auction      `json:"auctions"`
}

type auctionRealm struct {
	Name string    `json:"name"`
	Slug realmSlug `json:"slug"`
}

type auction struct {
	Auc        int64  `json:"auc"`
	Item       int64  `json:"item"`
	Owner      string `json:"owner"`
	PwnerRealm string `json:"ownerRealm"`
	Bid        int64  `json:"bid"`
	Buyout     int64  `json:"buyout"`
	Quantity   int64  `json:"quantity"`
	TimeLeft   string `json:"timeLeft"`
	Rand       int64  `json:"rand"`
	Seed       int64  `json:"seed"`
	Context    int64  `json:"context"`
}
