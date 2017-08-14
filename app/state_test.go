package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestListenForStatus(t *testing.T) {
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
	sta.status = s

	// setting up a listener for responding to status requests
	stop := make(chan interface{})
	err = sta.listenForStatus(stop)
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
