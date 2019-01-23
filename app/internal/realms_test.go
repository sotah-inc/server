package internal

import (
	"fmt"
	"testing"
	"time"

	"github.com/sotah-inc/server/app/pkg/messenger"

	"github.com/sotah-inc/server/app/pkg/state"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/utiltest"
	"github.com/stretchr/testify/assert"
)

func TestRealmGetAuctions(t *testing.T) {
	// initial Resolver
	res := Resolver{Config: &Config{}}

	// setting up the Resolver urls
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

	s, err := newStatusFromFilepath(Region{}, "./TestData/Realm-status.json")
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
	// initial Resolver
	res := Resolver{Config: &Config{}}

	// setting up a test status
	s, err := newStatusFromFilepath(Region{}, "./TestData/Realm-status.json")
	if !assert.Nil(t, err) {
		return
	}
	if !assert.NotEmpty(t, s.Realms) {
		return
	}

	// setting up the Resolver urls
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

	// creating a Realm whitelist
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
			if !assert.True(t, whitelistOk, fmt.Sprintf("Job for invalid Realm found: %s", job.realm.Slug)) {
				return
			}
		}

		if out == nil {
			break
		}
	}
}

func TestRealmsGetAllAuctions(t *testing.T) {
	// initial Resolver
	res := Resolver{Config: &Config{}}

	// setting up a test status
	s, err := newStatusFromFilepath(Region{}, "./TestData/Realm-status.json")
	if !assert.Nil(t, err) {
		return
	}
	if !assert.NotEmpty(t, s.Realms) {
		return
	}

	// setting up the Resolver urls
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

func validateStatus(t *testing.T, reg Region, s status) bool {
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
	reg := Region{Hostname: "us.battle.net"}
	s, err := newStatusFromFilepath(reg, "./TestData/Realm-status.json")
	if !assert.Nil(t, err) {
		return
	}
	if !validateStatus(t, reg, s) {
		return
	}
}

func TestNewStatusFromMessenger(t *testing.T) {
	sta := state.State{}

	// connecting
	mess, err := messenger.NewMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}
	sta.Messenger = mess

	// building test status
	reg := Region{Name: "us", Hostname: "us.battle.net"}
	s, err := newStatusFromFilepath(reg, "./TestData/Realm-status.json")
	if !assert.Nil(t, err) {
		return
	}
	if !validateStatus(t, reg, s) {
		return
	}
	sta.Statuses = map[RegionName]status{reg.Name: s}
	sta.Regions = []Region{reg}

	// setting up a subscriber that will publish status retrieval requests
	stop := make(chan interface{})
	err = sta.listenForStatus(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive Statuses
	receivedStatus, err := newStatusFromMessenger(reg, mess)
	if !assert.Nil(t, err) || !assert.Equal(t, s.region.Hostname, receivedStatus.region.Hostname) {
		stop <- struct{}{}
		return
	}

	// flagging the status listener to exit
	stop <- struct{}{}
}
func TestNewStatus(t *testing.T) {
	blizzStatus, err := blizzard.NewStatusFromFilepath("./TestData/Realm-status.json")
	if !assert.Nil(t, err) {
		return
	}

	reg := Region{Hostname: "us.battle.net"}
	s := newStatus(reg, blizzStatus)
	if !validateStatus(t, reg, s) {
		return
	}
}
