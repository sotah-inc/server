package blizzard

import (
	"testing"

	"github.com/sotah-inc/server/app/utiltest"
	"github.com/stretchr/testify/assert"
)

func validateAuctions(a Auctions) bool {
	if len(a.Realms) == 0 {
		return false
	}

	return true
}

func TestNewAuctionInfoFromHTTP(t *testing.T) {
	ts, err := utiltest.ServeFile("../TestData/auctioninfo.json")
	if !assert.Nil(t, err) {
		return
	}

	a, _, err := NewAuctionInfoFromHTTP(ts.URL)
	if !assert.Nil(t, err) {
		return
	}
	if !assert.NotEmpty(t, a.Files) {
		return
	}
}

func TestNewAuctionInfoFromFilepath(t *testing.T) {
	a, err := NewAuctionInfoFromFilepath("../TestData/auctioninfo.json")
	if !assert.Nil(t, err) {
		return
	}
	if !assert.NotEmpty(t, a.Files) {
		return
	}
}

func TestNewAuctionInfo(t *testing.T) {
	body, err := utiltest.ReadFile("../TestData/auctioninfo.json")
	if !assert.Nil(t, err) {
		return
	}

	a, err := NewAuctionInfo(body)
	if !assert.Nil(t, err) {
		return
	}
	if !assert.NotEmpty(t, a.Files) {
		return
	}
}

func TestGetAuctions(t *testing.T) {
	// setting up the resolver urls
	auctionsTs, err := utiltest.ServeFile("../TestData/auctions.json")
	if !assert.Nil(t, err) {
		return
	}

	// gathering the auction-info
	auctionInfo, err := NewAuctionInfoFromFilepath("../TestData/auctioninfo.json")
	if !assert.Nil(t, err) {
		return
	}
	if !assert.NotEmpty(t, auctionInfo.Files) {
		return
	}

	// overriding the url
	auctionInfo.Files[0].URL = auctionsTs.URL

	// gathering the auctions
	auctions, err := auctionInfo.Files[0].GetAuctions()
	if !assert.Nil(t, err) {
		return
	}
	if !assert.NotEmpty(t, auctions.Auctions) {
		return
	}
}

func TestGetFirstAuctions(t *testing.T) {
	// setting up the resolver urls
	auctionInfoTs, err := utiltest.ServeFile("../TestData/auctioninfo.json")
	if !assert.Nil(t, err) {
		return
	}
	auctionsTs, err := utiltest.ServeFile("../TestData/auctions.json")
	if !assert.Nil(t, err) {
		return
	}

	// gathering an auction-info
	auctionInfo, _, err := NewAuctionInfoFromHTTP(auctionInfoTs.URL)
	if !assert.Nil(t, err) {
		return
	}
	if !assert.NotEmpty(t, auctionInfo.Files) {
		return
	}

	// overriding the url field
	auctionInfo.Files[0].URL = auctionsTs.URL

	// gathering the auctinos
	auctions, err := auctionInfo.GetFirstAuctions()
	if !assert.Nil(t, err) {
		return
	}
	if !assert.NotEmpty(t, auctions.Auctions) {
		return
	}
}

func TestNewAuctionsFromHTTP(t *testing.T) {
	ts, err := utiltest.ServeFile("../TestData/auctions.json")
	if !assert.Nil(t, err) {
		return
	}

	a, err := NewAuctionsFromHTTP(ts.URL)
	if !assert.Nil(t, err) {
		return
	}
	if !assert.True(t, validateAuctions(a)) {
		return
	}
}
func TestNewAuctionsFromFilepath(t *testing.T) {
	a, err := NewAuctionsFromFilepath("../TestData/auctions.json")
	if !assert.Nil(t, err) {
		return
	}
	if !assert.True(t, validateAuctions(a)) {
		return
	}
}

func TestNewAuctions(t *testing.T) {
	body, err := utiltest.ReadFile("../TestData/auctions.json")
	if !assert.Nil(t, err) {
		return
	}

	a, err := NewAuctions(body)
	if !assert.Nil(t, err) {
		return
	}
	if !assert.True(t, validateAuctions(a)) {
		return
	}
}
