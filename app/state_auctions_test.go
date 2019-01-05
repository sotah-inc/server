package main

import (
	"testing"

	"github.com/sotah-inc/server/app/blizzard"
	"github.com/sotah-inc/server/app/sortdirections"
	"github.com/sotah-inc/server/app/sortkinds"
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
	aucs, err := blizzard.NewAuctionsFromFilepath("./TestData/auctions.json")
	if !assert.Nil(t, err) {
		return
	}

	// building a test realm
	reg := region{Name: "us"}
	rea, err := blizzard.NewRealmFromFilepath("./TestData/realm.json")
	if !assert.Nil(t, err) {
		return
	}

	// setting up a subscriber that will publish auctions
	stop := make(chan interface{})
	err = sta.listenForAuctions(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive auctions
	receivedMiniAuctions, err := newMiniAuctionsListFromMessenger(newMiniAuctionsListFromMessengerConfig{
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
	if !assert.Equal(t, len(aucs.Auctions), len(receivedMiniAuctions)) {
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
	_, err = blizzard.NewAuctionsFromFilepath("./TestData/auctions.json")
	if !assert.Nil(t, err) {
		return
	}

	// building sorted test auctions
	aSorted, err := newMiniAuctionsDataFromFilepath("./TestData/mini-auctions-sort-by-item.json")
	if !assert.Nil(t, err) {
		return
	}

	// building a test realm
	reg := region{Name: "us"}
	rea, err := blizzard.NewRealmFromFilepath("./TestData/realm.json")
	if !assert.Nil(t, err) {
		return
	}

	// setting up a subscriber that will publish auctions
	stop := make(chan interface{})
	err = sta.listenForAuctions(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive auctions
	receivedMiniAuctions, err := newMiniAuctionsListFromMessenger(newMiniAuctionsListFromMessengerConfig{
		realm:         realm{Realm: rea, region: reg},
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
	_, err = blizzard.NewAuctionsFromFilepath("./TestData/auctions.json")
	if !assert.Nil(t, err) {
		return
	}

	// building filtered test auctions
	aFiltered, err := newMiniAuctionsDataFromFilepath("./TestData/mini-auctions-filtered-by-owner-name.json")
	if !assert.Nil(t, err) {
		return
	}

	// building a test realm
	reg := region{Name: "us"}
	rea, err := blizzard.NewRealmFromFilepath("./TestData/realm.json")
	if !assert.Nil(t, err) {
		return
	}

	// setting up a subscriber that will publish auctions
	stop := make(chan interface{})
	err = sta.listenForAuctions(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive auctions
	receivedMiniAuctions, err := newMiniAuctionsListFromMessenger(newMiniAuctionsListFromMessengerConfig{
		realm:         realm{Realm: rea, region: reg},
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
