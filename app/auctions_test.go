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

	a, err := newAuctions(ts.URL, func(url string) string {
		return url
	})
	if !assert.Nil(t, err) {
		return
	}
	if !assert.NotEmpty(t, a.Realms) {
		return
	}
}
