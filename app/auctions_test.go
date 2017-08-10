package app

import (
	"testing"

	"github.com/ihsw/go-download/app/utiltest"
	"github.com/stretchr/testify/assert"
)

func TestNewAuctionsFromHTTP(t *testing.T) {
	ts, err := utiltest.ServeFile("./TestData/auctions.json")
	if !assert.Nil(t, err) {
		return
	}

	a, err := newAuctionsFromHTTP(
		ts.URL,
		resolver{getAuctionsURL: func(url string) string { return url }},
	)
	if !assert.Nil(t, err) {
		return
	}
	if !assert.NotEmpty(t, a.Realms) {
		return
	}
}

func TestNewAuctions(t *testing.T) {
	body, err := utiltest.ReadFile("./TestData/auctions.json")
	if !assert.Nil(t, err) {
		return
	}

	a, err := newAuctions(body)
	if !assert.Nil(t, err) {
		return
	}
	if !assert.NotEmpty(t, a.Realms) {
		return
	}
}
