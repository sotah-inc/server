package app

import (
	"fmt"
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

func TestStatusListen(t *testing.T) {
	// parsing env vars
	natsHost := os.Getenv("NATS_HOST")
	if !assert.NotEmpty(t, natsHost) {
		return
	}
	natsPort, err := strconv.Atoi(os.Getenv("NATS_PORT"))
	if !assert.Nil(t, err) || !assert.NotEmpty(t, natsPort) {
		return
	}

	// connecting the messenger
	mess, err := newMessenger(natsHost, natsPort)
	if !assert.Nil(t, err) {
		return
	}

	// fetching a status
	realmStatusTs, err := utiltest.ServeFile("./TestData/realm-status.json")
	if !assert.Nil(t, err) {
		return
	}
	reg := region{Hostname: "us.battle.net"}
	sta, err := newStatusFromHTTP(
		reg,
		resolver{getStatusURL: func(regionHostname string) string { return realmStatusTs.URL }},
	)
	if !assert.NotEmpty(t, sta.Realms) {
		return
	}

	// filling the messenger with the status and listening
	mess.status = sta
	_, err = mess.statusListen()
	if !assert.Nil(t, err) {
		return
	}

	// requesting the status
	requestedStatus, err := mess.requestStatus()
	if !assert.Nil(t, err) || !assert.Equal(t, sta.region.Hostname, requestedStatus.region.Hostname) {
		return
	}
	if !assert.NotZero(t, len(requestedStatus.Realms)) {
		return
	}
	if !assert.Equal(t, len(sta.Realms), len(requestedStatus.Realms)) {
		return
	}
	for i, rea := range sta.Realms {
		requestedRealm := requestedStatus.Realms[i]
		fmt.Printf("comparing %s with %s\n", rea.region.Hostname, requestedRealm.region.Hostname)
		if !assert.Equal(t, rea.region.Hostname, requestedRealm.region.Hostname) {
			return
		}
	}
}
