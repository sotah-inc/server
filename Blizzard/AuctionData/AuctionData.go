package AuctionData

import (
	"encoding/json"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Util"
)

/*
	blizzard json response structs
*/
type Response struct {
	Realm    ResponseRealm `json:realm`
	Alliance Auctions      `json:alliance`
	Horde    Auctions      `json:horde`
	Neutral  Auctions      `json:neutral`
}

type ResponseRealm struct {
	Name string `json:name`
	Slug string `json:slug`
}

type Auctions struct {
	Auctions []Auction `json:auctions`
}

type Auction struct {
	Auc        uint64 `json:auc`
	Item       uint64 `json:item`
	Owner      string `json:owner`
	OwnerRealm string `json:ownerRealm`
	Bid        uint64 `json:bid`
	Buyout     uint64 `json:buyout`
	Quantity   uint64 `json:quantity`
	TimeLeft   string `json:timeLeft`
	Rand       int64  `json:rand`
	Seed       uint64 `json:seed`
}

/*
	funcs
*/
func Get(realm Entity.Realm, url string) (response Response, err error) {
	var b []byte
	b, err = Util.Download(url)
	if err != nil {
		return
	}

	err = json.Unmarshal(b, &response)
	if err != nil {
		return
	}

	return response, nil
}
