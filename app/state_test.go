package main

import (
	"testing"

	"github.com/ihsw/sotah-server/app/codes"

	"github.com/ihsw/sotah-server/app/subjects"

	"github.com/stretchr/testify/assert"
)

func TestListenForStatus(t *testing.T) {
	sta := State{}

	// connecting
	mess, err := newMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}
	sta.messenger = mess

	// building test status
	reg := region{Hostname: "us.battle.net"}
	s, err := NewStatusFromFilepath(reg, "./TestData/realm-status.json")
	if !assert.Nil(t, err) {
		return
	}
	if !validateStatus(t, reg, s) {
		return
	}
	sta.Statuses = map[regionName]*Status{reg.Name: s}

	// setting up a listener for responding to status requests
	stop := make(chan interface{})
	err = sta.ListenForStatus(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive statuses
	receivedStatus, err := newStatusFromMessenger(reg, mess)
	if !assert.Nil(t, err) || !assert.Equal(t, s.region.Hostname, receivedStatus.region.Hostname) {
		return
	}

	// flagging the status listener to exit
	stop <- struct{}{}
}

func TestListenForAuctions(t *testing.T) {
	sta := State{}

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
	sta.auctions = map[regionName]map[realmSlug]*auctions{
		reg.Name: map[realmSlug]*auctions{
			rea.Slug: a,
		},
	}

	// setting up a subscriber that will publish auctions
	stop := make(chan interface{})
	err = sta.ListenForAuctions(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive auctions
	receivedAuctions, err := newAuctionsFromMessenger(rea, mess)
	if !assert.Nil(t, err) {
		return
	}
	if !assert.NotZero(t, len(receivedAuctions.Auctions)) {
		return
	}
	if !assert.Equal(t, len(a.Auctions), len(receivedAuctions.Auctions)) {
		return
	}
}

func TestListenForRegions(t *testing.T) {
	sta := State{}

	// connecting
	mess, err := newMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}
	sta.messenger = mess

	// building test status
	reg := region{Hostname: "us.battle.net"}
	s, err := NewStatusFromFilepath(reg, "./TestData/realm-status.json")
	if !assert.Nil(t, err) {
		return
	}
	if !validateStatus(t, reg, s) {
		return
	}
	sta.Statuses = map[regionName]*Status{reg.Name: s}

	// building test config
	c, err := newConfigFromFilepath("./TestData/config.json")
	if !assert.Nil(t, err) || !assert.NotEmpty(t, c.APIKey) {
		return
	}
	sta.config = c

	// setting up a listener for responding to status requests
	stop := make(chan interface{})
	err = sta.listenForRegions(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive regions
	regs, err := newRegionsFromMessenger(mess)
	if !assert.Nil(t, err) || !assert.NotZero(t, len(regs)) {
		return
	}

	// flagging the status listener to exit
	stop <- struct{}{}
}

func TestListenForGenericTestErrors(t *testing.T) {
	sta := state{}

	// connecting
	mess, err := newMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}
	sta.messenger = mess

	// setting up a listener for responding to status requests
	stop := make(chan interface{})
	err = sta.listenForGenericTestErrors(stop)
	if !assert.Nil(t, err) {
		return
	}

	// requesting a message from
	msg, err := sta.messenger.request(subjects.GenericTestErrors, []byte{})
	if !assert.Nil(t, err) {
		return
	}

	// flagging the status listener to exit
	stop <- struct{}{}

	if !assert.Equal(t, msg.Code, codes.GenericError) {
		return
	}
}
