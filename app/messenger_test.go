package app

import (
	"os"
	"strconv"
	"testing"

	"github.com/ihsw/go-download/app/utiltest"
	"github.com/stretchr/testify/assert"
)

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
	// resolving messenger host/port
	natsHost := os.Getenv("NATS_HOST")
	if !assert.NotEmpty(t, natsHost) {
		return
	}
	natsPort, err := strconv.Atoi(os.Getenv("NATS_PORT"))
	if !assert.Nil(t, err) || !assert.NotEmpty(t, natsPort) {
		return
	}

	// connecting
	mess, err := newMessenger(natsHost, natsPort)
	if !assert.Nil(t, err) {
		return
	}

	// fetching test status data
	body, err := utiltest.ReadFile("./TestData/realm-status.json")
	if !assert.Nil(t, err) {
		return
	}

	// building test status
	reg := region{Hostname: "us.battle.net"}
	s, err := newStatus(reg, body)
	if !assert.NotEmpty(t, s.Realms) {
		return
	}
	mess.status = s

	// setting up a subscriber that will publish status retrieval requests
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
