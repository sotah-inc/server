package AuctionData

import (
	"encoding/json"
	"fmt"
	"github.com/ihsw/go-download/Util"
)

/*
	misc
*/
type Response struct {
	Realm    ResponseRealm
	Alliance Auctions
	Horde    Auctions
	Neutral  Auctions
}

type ResponseRealm struct {
	Name string
	Slug string
}

type Auctions struct {
	Auctions []Auction
}

type Auction struct {
	Auc        uint64
	Item       uint64
	Owner      string
	OwnerRealm string
	Bid        uint64
	Buyout     uint64
	Quantity   uint64
	TimeLeft   string
	Rand       int64
	Seed       uint64
}

/*
	funcs
*/
func Get(url string) (Response, error) {
	var (
		b        []byte
		response Response
		err      error
	)

	fmt.Println(fmt.Sprintf("start %s", url))
	b, err = Util.Download(url)
	fmt.Println(fmt.Sprintf("end %s", url))
	if err != nil {
		return response, err
	}

	err = json.Unmarshal(b, &response)
	if err != nil {
		return response, err
	}

	return response, nil
}
