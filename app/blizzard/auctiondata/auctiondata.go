package auctiondata

import (
	"encoding/json"

	"github.com/ihsw/go-download/app/entity"
	"github.com/ihsw/go-download/app/util"
)

/*
	Response
*/
type Response struct {
	Realm    ResponseRealm `json:realm`
	Auctions Auctions      `json:auctions`
}

/*
	ResponseRealm
*/
type ResponseRealm struct {
	Name string `json:name`
	Slug string `json:slug`
}

/*
	Auctions
*/
type Auctions struct {
	Auctions []Auction `json:auctions`
}

/*
	Auction
*/
type Auction struct {
	Auc        int64  `json:auc`
	Item       int64  `json:item`
	Owner      string `json:owner`
	OwnerRealm string `json:ownerRealm`
	Bid        int64  `json:bid`
	Buyout     int64  `json:buyout`
	Quantity   int64  `json:quantity`
	TimeLeft   string `json:timeLeft`
	Rand       int64  `json:rand`
	Seed       int64  `json:seed`
}

/*
	funcs
*/
func Get(realm entity.Realm, url string) (response *Response) {
	var (
		b   []byte
		err error
	)
	b, err = util.Download(url)
	if err != nil {
		return nil
	}

	err = json.Unmarshal(b, &response)
	if err != nil {
		return nil
	}

	return response
}
