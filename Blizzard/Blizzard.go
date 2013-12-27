package Blizzard

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ihsw/go-download/Blizzard/Auction"
	_ "github.com/ihsw/go-download/Blizzard/AuctionData"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Util"
)

func DownloadRealm(realm Entity.Realm, c chan Auction.Result) {
	var (
		b               []byte
		auctionResponse Auction.Response
	)
	auctionResult := Auction.Result{
		Response: auctionResponse,
		Realm:    realm,
		Length:   0,
		Error:    nil,
	}

	url := fmt.Sprintf(Auction.URL_FORMAT, realm.Region.Host, realm.Slug)
	b, auctionResult.Error = Util.Download(url)
	if auctionResult.Error != nil {
		c <- auctionResult
		return
	}

	auctionResult.Error = json.Unmarshal(b, &auctionResponse)
	if auctionResult.Error != nil {
		c <- auctionResult
		return
	}

	if len(auctionResponse.Files) == 0 {
		auctionResult.Error = errors.New("Response.Files length was zero")
		c <- auctionResult
		return
	} else if len(auctionResponse.Files) > 1 {
		auctionResult.Error = errors.New("Response.Files length was >1")
		c <- auctionResult
		return
	}

	auctionResult.Response = auctionResponse
	auctionResult.Length = int64(len(b))
	c <- auctionResult
}
