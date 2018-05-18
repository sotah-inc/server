package main

import (
	"testing"

	"github.com/ihsw/sotah-server/app/codes"

	"github.com/ihsw/sotah-server/app/subjects"

	"github.com/stretchr/testify/assert"
)

func TestListenForRegions(t *testing.T) {
	sta := state{}

	// connecting
	mess, err := newMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}
	sta.messenger = mess

	// building test status
	reg := region{Hostname: "us.battle.net"}
	s, err := newStatusFromFilepath(reg, "./TestData/realm-status.json")
	if !assert.Nil(t, err) {
		return
	}
	if !validateStatus(t, reg, s) {
		return
	}
	sta.statuses = map[regionName]status{reg.Name: s}

	// building test config
	c, err := newConfigFromFilepath("./TestData/config.json")
	if !assert.Nil(t, err) || !assert.NotEmpty(t, c.APIKey) {
		return
	}
	sta.regions = c.Regions

	// setting up a listener for responding to status requests
	stop := make(chan interface{})
	err = sta.listenForRegions(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive regions
	regs, err := newRegionsFromMessenger(mess)
	if !assert.Nil(t, err) || !assert.NotZero(t, len(regs)) {
		stop <- struct{}{}

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
		stop <- struct{}{}

		return
	}

	// validating the response code
	if !assert.Equal(t, msg.Code, codes.GenericError) {
		return
	}

	// flagging the status listener to exit
	stop <- struct{}{}
}
