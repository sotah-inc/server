package main

import (
	"testing"

	"github.com/ihsw/sotah-server/app/sortdirections"
	"github.com/ihsw/sotah-server/app/sortkinds"
	"github.com/stretchr/testify/assert"
)

func TestListenForAuctions(t *testing.T) {
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
	rea, err := newRealmFromFilepath(reg, "./TestData/realm.json")
	if !assert.Nil(t, err) {
		return
	}

	// attaching the auctions to the state
	sta.auctions = map[regionName]map[realmSlug]miniAuctionList{
		reg.Name: map[realmSlug]miniAuctionList{
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
		realm:     rea,
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

	stop <- struct{}{}
}

func TestListenForSortedAuctions(t *testing.T) {
	t.Skip("TODO after creating blizzard package")

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

	// building sorted test auctions
	aSorted, err := newMiniAuctionsDataFromFilepath("./TestData/mini-auctions-sort-by-item.json")
	if !assert.Nil(t, err) {
		return
	}
	if !assert.True(t, validateAuctions(a)) {
		return
	}

	// building a test realm
	reg := region{Name: "us"}
	rea, err := newRealmFromFilepath(reg, "./TestData/realm.json")
	if !assert.Nil(t, err) {
		return
	}

	// attaching the auctions to the state
	sta.auctions = map[regionName]map[realmSlug]miniAuctionList{
		reg.Name: map[realmSlug]miniAuctionList{
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
		realm:         rea,
		messenger:     mess,
		sortKind:      sortkinds.Item,
		sortDirection: sortdirections.Up,
		count:         10,
	})
	if !assert.Nil(t, err) {
		stop <- struct{}{}

		return
	}
	if !assert.NotZero(t, len(receivedMiniAuctions)) {
		stop <- struct{}{}

		return
	}
	if !assert.Equal(t, aSorted.Auctions, receivedMiniAuctions) {
		stop <- struct{}{}

		return
	}

	stop <- struct{}{}
}

func TestListenForAuctionsFilteredByOwnerName(t *testing.T) {
	t.Skip("TODO after creating blizzard package")

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

	// building filtered test auctions
	aFiltered, err := newMiniAuctionsDataFromFilepath("./TestData/mini-auctions-filtered-by-owner-name.json")
	if !assert.Nil(t, err) {
		return
	}
	if !assert.True(t, validateAuctions(a)) {
		return
	}

	// building a test realm
	reg := region{Name: "us"}
	rea, err := newRealmFromFilepath(reg, "./TestData/realm.json")
	if !assert.Nil(t, err) {
		return
	}

	// attaching the auctions to the state
	sta.auctions = map[regionName]map[realmSlug]miniAuctionList{
		reg.Name: map[realmSlug]miniAuctionList{
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
		realm:         rea,
		messenger:     mess,
		sortKind:      sortkinds.Item,
		sortDirection: sortdirections.Up,
		count:         10,
		ownerFilter:   "Lunarhawk",
	})
	if !assert.Nil(t, err) {
		stop <- struct{}{}

		return
	}
	if !assert.NotZero(t, len(receivedMiniAuctions)) {
		stop <- struct{}{}

		return
	}
	if !assert.Equal(t, aFiltered.Auctions, receivedMiniAuctions) {
		stop <- struct{}{}

		return
	}

	stop <- struct{}{}
}
