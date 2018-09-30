package main

import (
	"testing"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/stretchr/testify/assert"
)

func TestListenForOwners(t *testing.T) {
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

	// building a test realm
	reg := region{Name: "us"}
	rea, err := blizzard.NewRealmFromFilepath("./TestData/realm.json")
	if !assert.Nil(t, err) {
		return
	}

	// fetching expected owners
	expectedOwners, err := newOwnersFromFilepath("./TestData/owners.json")
	if !assert.Nil(t, err) {
		return
	}

	// setting up a subscriber that will publish auctions
	stop := make(chan interface{})
	err = sta.listenForOwners(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive auctions
	receivedOwners, err := newOwnersFromMessenger(mess, ownersRequest{reg.Name, rea.Slug, ""})
	if !assert.Nil(t, err) {
		stop <- struct{}{}

		return
	}
	if !assert.NotZero(t, len(receivedOwners.Owners)) {
		stop <- struct{}{}

		return
	}

	if !assert.Equal(t, expectedOwners, receivedOwners) {
		stop <- struct{}{}

		return
	}

	stop <- struct{}{}
}

func TestListenForOwnersFiltered(t *testing.T) {
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

	// building a test realm
	reg := region{Name: "us"}
	rea, err := blizzard.NewRealmFromFilepath("./TestData/realm.json")
	if !assert.Nil(t, err) {
		return
	}

	// fetching expected owners
	expectedOwners, err := newOwnersFromFilepath("./TestData/owners-filtered.json")
	if !assert.Nil(t, err) {
		return
	}

	// setting up a subscriber that will publish auctions
	stop := make(chan interface{})
	err = sta.listenForOwners(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive auctions
	receivedOwners, err := newOwnersFromMessenger(mess, ownersRequest{reg.Name, rea.Slug, "lunar"})
	if !assert.Nil(t, err) {
		stop <- struct{}{}

		return
	}
	if !assert.NotZero(t, len(receivedOwners.Owners)) {
		stop <- struct{}{}

		return
	}

	if !assert.Equal(t, expectedOwners, receivedOwners) {
		stop <- struct{}{}

		return
	}

	stop <- struct{}{}
}
