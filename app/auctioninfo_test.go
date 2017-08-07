package app

import (
	"testing"

	"github.com/ihsw/go-download/app/utiltest"
	"github.com/stretchr/testify/assert"
)

func TestNewAuctions(t *testing.T) {
	ts, err := utiltest.ServeFile("./TestData/auctions.json")
	if !assert.Nil(t, err) {
		return
	}

	a, err := newAuctionInfo("", "", func(regionName string, realmName string) string {
		return ts.URL
	})
	if !assert.Nil(t, err) {
		return
	}
	if !assert.NotEmpty(t, a.Files) {
		return
	}
}
