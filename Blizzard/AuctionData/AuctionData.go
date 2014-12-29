package AuctionData

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Util"
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
func Get(realm Entity.Realm, url string) (response Response, err error) {
	var b []byte
	b, err = Util.Download(url)
	if err != nil {
		err = errors.New(fmt.Sprintf("Util.Download() failed (%s)", err.Error()))
		return
	}

	err = json.Unmarshal(b, &response)
	if err != nil {
		err = errors.New(fmt.Sprintf("json.Unmarshal() for %s failed (%s)", url, err.Error()))
		return
	}

	return response, nil
}
