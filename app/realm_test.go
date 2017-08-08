package app

import (
	"testing"
	"time"

	"github.com/ihsw/go-download/app/utiltest"
	"github.com/stretchr/testify/assert"
)

func TestRealmGetAuctions(t *testing.T) {
	// initial resolver
	res := resolver{}

	// setting up the resolver urls
	realmStatusTs, err := utiltest.ServeFile("./TestData/realm-status.json")
	if !assert.Nil(t, err) {
		return
	}
	res.getStatusURL = func(regionHostname string) string { return realmStatusTs.URL }
	auctionInfoTs, err := utiltest.ServeFile("./TestData/auctioninfo.json")
	if !assert.Nil(t, err) {
		return
	}
	res.getAuctionInfoURL = func(regionHostname string, slug realmSlug) string {
		return auctionInfoTs.URL
	}
	auctionsTs, err := utiltest.ServeFile("./TestData/auctions.json")
	if !assert.Nil(t, err) {
		return
	}
	res.getAuctionsURL = func(url string) string {
		return auctionsTs.URL
	}

	s, err := newStatus(region{}, res)
	if !assert.NotEmpty(t, s.Realms) {
		return
	}

	rea := s.Realms[0]

	aucs, err := rea.getAuctions(res)
	if !assert.Nil(t, err) {
		return
	}
	if !assert.NotEmpty(t, aucs.Realms) {
		return
	}
}

func TestRealmsGetAuctions(t *testing.T) {
	// initial resolver
	res := resolver{}

	// setting up the resolver urls
	realmStatusTs, err := utiltest.ServeFile("./TestData/realm-status.json")
	if !assert.Nil(t, err) {
		return
	}
	res.getStatusURL = func(regionHostname string) string { return realmStatusTs.URL }
	auctionInfoTs, err := utiltest.ServeFile("./TestData/auctioninfo.json")
	if !assert.Nil(t, err) {
		return
	}
	res.getAuctionInfoURL = func(regionHostname string, slug realmSlug) string {
		return auctionInfoTs.URL
	}
	auctionsTs, err := utiltest.ServeFile("./TestData/auctions.json")
	if !assert.Nil(t, err) {
		return
	}
	res.getAuctionsURL = func(url string) string {
		return auctionsTs.URL
	}

	s, err := newStatus(region{}, res)
	if !assert.NotEmpty(t, s.Realms) {
		return
	}

	timer := time.After(5 * time.Second)
	out := s.Realms.getAuctions(res)
	for {
		select {
		case <-timer:
			t.Fatal("Timed out")
		case job, ok := <-out:
			if !ok {
				out = nil
				break
			}

			if !assert.Nil(t, job.err) {
				return
			}
		}

		if out == nil {
			break
		}
	}
}
