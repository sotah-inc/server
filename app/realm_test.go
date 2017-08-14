package main

import (
	"testing"
	"time"

	"github.com/ihsw/go-download/app/utiltest"
	"github.com/stretchr/testify/assert"
)

func TestNewRealmFromFilepath(t *testing.T) {
	_, err := newRealmFromFilepath(region{}, "./TestData/realm.json")
	if !assert.Nil(t, err) {
		return
	}
}

func TestNewRealm(t *testing.T) {
	body, err := utiltest.ReadFile("./TestData/realm.json")
	if !assert.Nil(t, err) {
		return
	}

	_, err = newRealm(region{}, body)
	if !assert.Nil(t, err) {
		return
	}
}

func TestRealmGetAuctions(t *testing.T) {
	// initial resolver
	res := resolver{}

	// setting up the resolver urls
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

	s, err := newStatusFromFilepath(region{}, "./TestData/realm-status.json")
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

	s, err := newStatusFromFilepath(region{}, "./TestData/realm-status.json")
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
