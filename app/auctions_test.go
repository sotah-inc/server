package main

import (
	"testing"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/utiltest"
	"github.com/stretchr/testify/assert"
)

func validateAuctions(a auctions) bool {
	if len(a.Realms) == 0 {
		return false
	}

	return true
}

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
	if !assert.True(t, validateAuctions(a)) {
		return
	}
}
func TestNewAuctionsFromFilepath(t *testing.T) {
	a, err := newAuctionsFromFilepath("./TestData/auctions.json")
	if !assert.Nil(t, err) {
		return
	}
	if !assert.True(t, validateAuctions(a)) {
		return
	}
}

func TestNewMiniAuctionsDataFromFilepath(t *testing.T) {
	t.Skip("TODO after creating blizzard package")

	mad, err := newMiniAuctionsDataFromFilepath("./TestData/mini-auctions.json")
	if !assert.Nil(t, err) {
		return
	}

	if !assert.Equal(t, 3, len(mad.Auctions)) {
		return
	}
}

func TestNewMiniAuctionsFromMessenger(t *testing.T) {
	sta := state{}

	// connecting
	mess, err := newMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}
	sta.messenger = mess

	// building test auctions
	a, err := newAuctionsFromFilepath("./TestData/auctions.json")
	if !assert.Nil(t, err) {
		return
	}
	if !assert.True(t, validateAuctions(a)) {
		return
	}

	// building a test realm
	reg := region{Name: "us"}
	rea, err := blizzard.NewRealmFromFilepath("./TestData/realm.json")
	if !assert.Nil(t, err) {
		return
	}

	// attaching the auctions to the state
	sta.auctions = map[regionName]map[blizzard.RealmSlug]miniAuctionList{
		reg.Name: {
			rea.Slug: a.Auctions.minimize(),
		},
	}

	// setting up a subscriber that will publish auctions
	stop := make(chan interface{})
	err = sta.listenForAuctions(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive auctions
	receivedMiniAuctions, err := newMiniAuctionsFromMessenger(newMiniAuctionsFromMessengerConfig{
		realm:     realm{Realm: rea, region: reg},
		messenger: mess,
		count:     10,
	})
	if !assert.Nil(t, err) {
		stop <- struct{}{}

		return
	}
	if !assert.NotZero(t, len(receivedMiniAuctions)) {
		stop <- struct{}{}

		return
	}
	if !assert.Equal(t, len(a.Auctions), len(receivedMiniAuctions)) {
		stop <- struct{}{}

		return
	}

	// flagging the status listener to exit
	stop <- struct{}{}
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
	if !assert.True(t, validateAuctions(a)) {
		return
	}
}
