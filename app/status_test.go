package app

import (
	"testing"

	"github.com/ihsw/go-download/app/utiltest"
	"github.com/stretchr/testify/assert"
)

func validateStatus(t *testing.T, reg region, s *status) bool {
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

func TestNewStatusFromHTTP(t *testing.T) {
	ts, err := utiltest.ServeFile("./TestData/realm-status.json")
	if !assert.Nil(t, err) {
		return
	}

	reg := region{Hostname: "us.battle.net"}
	s, err := newStatusFromHTTP(
		reg,
		resolver{getStatusURL: func(regionHostname string) string { return ts.URL }},
	)
	if !assert.NotNil(t, err) {
		return
	}
	if !validateStatus(t, reg, s) {
		return
	}
}

func TestNewStatusFromFilepath(t *testing.T) {
	reg := region{Hostname: "us.battle.net"}
	s, err := newStatusFromFilepath(reg, "./TestData/realm-status.json")
	if !assert.NotNil(t, err) {
		return
	}
	if !validateStatus(t, reg, s) {
		return
	}
}

func TestNewStatusFromMessenger(t *testing.T) {
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
func TestNewStatus(t *testing.T) {
	body, err := utiltest.ReadFile("./TestData/realm-status.json")
	if !assert.Nil(t, err) {
		return
	}

	reg := region{Hostname: "us.battle.net"}
	s, err := newStatus(reg, body)
	if !assert.NotNil(t, err) {
		return
	}
	if !validateStatus(t, reg, s) {
		return
	}
}
