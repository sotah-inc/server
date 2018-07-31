package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/utiltest"
	"github.com/stretchr/testify/assert"
)

func TestRealmGetAuctions(t *testing.T) {
	// initial resolver
	res := resolver{config: &config{UseCacheDir: false}}

	// setting up the resolver urls
	auctionInfoTs, err := utiltest.ServeFile("./TestData/auctioninfo.json")
	if !assert.Nil(t, err) {
		return
	}
	res.getAuctionInfoURL = func(regionHostname string, slug blizzard.RealmSlug) string {
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
	if !assert.Nil(t, err) {
		return
	}
	if !assert.NotEmpty(t, s.Realms) {
		return
	}

	rea := s.Realms[0]

	aucs, _, err := rea.getAuctions(res)
	if !assert.Nil(t, err) {
		return
	}
	if !assert.NotEmpty(t, aucs.Realms) {
		return
	}
}

func TestRealmsGetAuctions(t *testing.T) {
	// initial resolver
	res := resolver{config: &config{UseCacheDir: false}}

	// setting up a test status
	s, err := newStatusFromFilepath(region{}, "./TestData/realm-status.json")
	if !assert.Nil(t, err) {
		return
	}
	if !assert.NotEmpty(t, s.Realms) {
		return
	}

	// setting up the resolver urls
	auctionInfoTs, err := utiltest.ServeFile("./TestData/auctioninfo.json")
	if !assert.Nil(t, err) {
		return
	}
	res.getAuctionInfoURL = func(regionHostname string, slug blizzard.RealmSlug) string {
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
	whitelist := map[blizzard.RealmSlug]interface{}{"earthen-ring": struct{}{}}

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
	res := resolver{config: &config{UseCacheDir: false}}

	// setting up a test status
	s, err := newStatusFromFilepath(region{}, "./TestData/realm-status.json")
	if !assert.Nil(t, err) {
		return
	}
	if !assert.NotEmpty(t, s.Realms) {
		return
	}

	// setting up the resolver urls
	auctionInfoTs, err := utiltest.ServeFile("./TestData/auctioninfo.json")
	if !assert.Nil(t, err) {
		return
	}
	res.getAuctionInfoURL = func(regionHostname string, slug blizzard.RealmSlug) string {
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

func validateStatus(t *testing.T, reg region, s status) bool {
	if !assert.NotEmpty(t, s.Realms) {
		return false
	}

	for _, rea := range s.Realms {
		if !assert.Equal(t, reg.Hostname, rea.region.Hostname) {
			return false
		}
	}

	return true
}

func TestNewStatusFromFilepath(t *testing.T) {
	reg := region{Hostname: "us.battle.net"}
	s, err := newStatusFromFilepath(reg, "./TestData/realm-status.json")
	if !assert.Nil(t, err) {
		return
	}
	if !validateStatus(t, reg, s) {
		return
	}
}

func TestNewStatusFromMessenger(t *testing.T) {
	sta := state{}

	// connecting
	mess, err := newMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}
	sta.messenger = mess

	// building test status
	reg := region{Name: "us", Hostname: "us.battle.net"}
	s, err := newStatusFromFilepath(reg, "./TestData/realm-status.json")
	if !assert.Nil(t, err) {
		return
	}
	if !validateStatus(t, reg, s) {
		return
	}
	sta.statuses = map[regionName]status{reg.Name: s}
	sta.regions = []region{reg}

	// setting up a subscriber that will publish status retrieval requests
	stop := make(chan interface{})
	err = sta.listenForStatus(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive statuses
	receivedStatus, err := newStatusFromMessenger(reg, mess)
	if !assert.Nil(t, err) || !assert.Equal(t, s.region.Hostname, receivedStatus.region.Hostname) {
		stop <- struct{}{}
		return
	}

	// flagging the status listener to exit
	stop <- struct{}{}
}
func TestNewStatus(t *testing.T) {
	blizzStatus, err := blizzard.NewStatusFromFilepath("./TestData/realm-status.json")
	if !assert.Nil(t, err) {
		return
	}

	reg := region{Hostname: "us.battle.net"}
	s := newStatus(reg, blizzStatus)
	if !validateStatus(t, reg, s) {
		return
	}
}
