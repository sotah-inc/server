package app

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMessengerFromEnvVars(t *testing.T) {
	_, err := newMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}
}

func TestNewMessenger(t *testing.T) {
	natsHost := os.Getenv("NATS_HOST")
	if !assert.NotEmpty(t, natsHost) {
		return
	}
	natsPort, err := strconv.Atoi(os.Getenv("NATS_PORT"))
	if !assert.Nil(t, err) || !assert.NotEmpty(t, natsPort) {
		return
	}

	_, err = newMessenger(natsHost, natsPort)
	if !assert.Nil(t, err) {
		return
	}
}

func TestListenForStatus(t *testing.T) {
	// connecting
	mess, err := newMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}

	// building test status
	reg := region{Hostname: "us.battle.net"}
	s, err := newStatusFromFilepath(reg, "./TestData/realm-status.json")
	if !assert.Nil(t, err) {
		return
	}
	if !validateStatus(t, reg, s) {
		return
	}
	mess.status = s

	// setting up a listener for responding to status requests
	stop := make(chan interface{})
	err = mess.listenForStatus(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive statuses
	receivedStatus, err := newStatusFromMessenger(reg, mess)
	if !assert.Nil(t, err) || !assert.Equal(t, s.region.Hostname, receivedStatus.region.Hostname) {
		return
	}
	stop <- struct{}{}
}
