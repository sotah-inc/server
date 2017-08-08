package app

import (
	"testing"

	"github.com/ihsw/go-download/app/utiltest"
	"github.com/stretchr/testify/assert"
)

func TestNewAuctionInfo(t *testing.T) {
	ts, err := utiltest.ServeFile("./TestData/auctioninfo.json")
	if !assert.Nil(t, err) {
		return
	}

	a, err := newAuctionInfo(
		realm{},
		resolver{getAuctionInfoURL: func(regionHostname string, slug realmSlug) string { return ts.URL }},
	)
	if !assert.Nil(t, err) {
		return
	}
	if !assert.NotEmpty(t, a.Files) {
		return
	}
}

func TestGetAuctions(t *testing.T) {
	// setting up the initial resolver
	r := resolver{}

	// setting up the resolver urls
	auctionInfoTs, err := utiltest.ServeFile("./TestData/auctioninfo.json")
	if !assert.Nil(t, err) {
		return
	}
	r.getAuctionInfoURL = func(regionHostname string, slug realmSlug) string {
		return auctionInfoTs.URL
	}
	auctionsTs, err := utiltest.ServeFile("./TestData/auctions.json")
	if !assert.Nil(t, err) {
		return
	}
	r.getAuctionsURL = func(url string) string {
		return auctionsTs.URL
	}

	auctionInfo, err := newAuctionInfo(realm{}, r)
	if !assert.Nil(t, err) {
		return
	}
	if !assert.NotEmpty(t, auctionInfo.Files) {
		return
	}

	auctions, err := auctionInfo.Files[0].getAuctions(r)
	if !assert.Nil(t, err) {
		return
	}
	if !assert.NotEmpty(t, auctions.Auctions) {
		return
	}
}

func TestGetFirstAuctions(t *testing.T) {
	// setting up the initial resolver
	r := resolver{}

	// setting up the resolver urls
	auctionInfoTs, err := utiltest.ServeFile("./TestData/auctioninfo.json")
	if !assert.Nil(t, err) {
		return
	}
	r.getAuctionInfoURL = func(regionHostname string, slug realmSlug) string {
		return auctionInfoTs.URL
	}
	auctionsTs, err := utiltest.ServeFile("./TestData/auctions.json")
	if !assert.Nil(t, err) {
		return
	}
	r.getAuctionsURL = func(url string) string {
		return auctionsTs.URL
	}

	auctionInfo, err := newAuctionInfo(realm{}, r)
	if !assert.Nil(t, err) {
		return
	}
	if !assert.NotEmpty(t, auctionInfo.Files) {
		return
	}

	auctions, err := auctionInfo.getFirstAuctions(r)
	if !assert.Nil(t, err) {
		return
	}
	if !assert.NotEmpty(t, auctions.Auctions) {
		return
	}
}
