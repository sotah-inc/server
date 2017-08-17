package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/ihsw/sotah-server/app/utiltest"
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

	// setting up a test status
	s, err := newStatusFromFilepath(region{}, "./TestData/realm-status.json")
	if !assert.NotEmpty(t, s.Realms) {
		return
	}

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

	// creating a realm whitelist
	whitelist := map[realmSlug]interface{}{"earthen-ring": struct{}{}}

	timer := time.After(5 * time.Second)
	out := s.Realms.getAuctions(res, whitelist)
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

			_, whitelistOk := whitelist[job.realm.Slug]
			if !assert.True(t, whitelistOk, fmt.Sprintf("Job for invalid realm found: %s", job.realm.Slug)) {
				return
			}
		}

		if out == nil {
			break
		}
	}
}

func TestRealmsGetAllAuctions(t *testing.T) {
	// initial resolver
	res := resolver{}

	// setting up a test status
	s, err := newStatusFromFilepath(region{}, "./TestData/realm-status.json")
	if !assert.NotEmpty(t, s.Realms) {
		return
	}

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

	timer := time.After(5 * time.Second)
	out := s.Realms.getAllAuctions(res)
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
