package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
	"github.com/ihsw/sotah-server/app/util"
	log "github.com/sirupsen/logrus"
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
	log.WithField("filepath", relativeFilepath).Info("Reading auctions from file")

	body, err := util.ReadFile(relativeFilepath)
	if err != nil {
		return nil, err
	}

	return newAuctions(body)
}

func newAuctionsFromGzFilepath(rea realm, relativeFilepath string) (*auctions, error) {
	body, err := util.ReadFile(relativeFilepath)
	if err != nil {
		return nil, err
	}

	decodedBody, err := util.GzipDecode(body)
	if err != nil {
		return nil, err
	}

	return newAuctions(decodedBody)
}

func newAuctionsFromMessenger(rea *realm, mess messenger) (*auctions, error) {
	am := auctionsRequest{
		RegionName: rea.region.Name,
		RealmSlug:  rea.Slug,
	}
	encodedMessage, err := json.Marshal(am)
	if err != nil {
		return nil, err
	}

	log.WithField("subject", subjects.Auctions).Info("Sending request")
	msg, err := mess.request(subjects.Auctions, encodedMessage)
	if err != nil {
		return nil, err
	}

	if msg.Code != codes.Ok {
		return nil, errors.New(msg.Err)
	}

	base64DecodedMessage, err := base64.StdEncoding.DecodeString(msg.Data)
	if err != nil {
		return nil, err
	}

	gzipDecodedMessage, err := util.GzipDecode(base64DecodedMessage)
	if err != nil {
		return nil, err
	}

	return newAuctions(gzipDecodedMessage)
}

func newAuctions(body []byte) (*auctions, error) {
	a := &auctions{}
	if err := json.Unmarshal(body, a); err != nil {
		return nil, err
	}

	return a, nil
}

type auctionList []auction

func (al auctionList) limit(count int, page int) (auctionList, error) {
	alLength := len(al)
	if alLength == 0 {
		return al, nil
	}

	start := page * count
	if start > alLength {
		return auctionList{}, fmt.Errorf("Start out of range: %d", start)
	}

	end := start + count
	if end > alLength {
		return al[start:], nil
	}

	return al[start:end], nil
}

type auctions struct {
	Realms   []auctionRealm `json:"realms"`
	Auctions auctionList    `json:"auctions"`
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
