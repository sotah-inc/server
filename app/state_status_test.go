package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sotah-inc/server/app/utiltest"
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
	reg := region{Name: "test", Hostname: "test"}
	s, err := newStatusFromFilepath(reg, "./TestData/realm-status.json")
	if !assert.Nil(t, err) {
		return
	}
	if !validateStatus(t, reg, s) {
		return
	}
	sta.regions = []region{reg}
	sta.statuses = map[regionName]status{reg.Name: s}

	// setting up a listener for responding to status requests
	stop := make(chan interface{})
	err = sta.listenForStatus(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive statuses
	receivedStatus, err := newStatusFromMessenger(reg, mess)
	if !assert.Nil(t, err) || !assert.Equal(t, s.region.Hostname, receivedStatus.region.Hostname) {
		stop <- struct{}{}

		return
	}

	// flagging the status listener to exit
	stop <- struct{}{}
}

func TestListenForNonexistentStatusNoResolver(t *testing.T) {
	sta := state{}

	// connecting
	mess, err := newMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}
	sta.messenger = mess
	sta.statuses = map[regionName]status{}

	// setting up a listener for responding to status requests
	stop := make(chan interface{})
	err = sta.listenForStatus(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive statuses
	_, err = newStatusFromMessenger(region{Name: "test", Hostname: "test"}, mess)
	if !assert.NotNil(t, err) && assert.Equal(t, "Invalid region", err.Error()) {
		stop <- struct{}{}

		return
	}

	// flagging the status listener to exit
	stop <- struct{}{}
}

func TestListenForNonexistentStatus(t *testing.T) {
	// listening for status requests
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))

	sta := state{
		resolver: resolver{
			getStatusURL: func(regionHostname string) string { return ts.URL },
		},
	}

	// connecting
	mess, err := newMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}
	sta.messenger = mess
	sta.statuses = map[regionName]status{}

	// setting up a listener for responding to status requests
	stop := make(chan interface{})
	err = sta.listenForStatus(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive statuses
	_, err = newStatusFromMessenger(region{Name: "test", Hostname: "test"}, mess)
	if !assert.NotNil(t, err) && assert.Equal(t, "Invalid region", err.Error()) {
		stop <- struct{}{}

		return
	}

	// flagging the status listener to exit
	stop <- struct{}{}
}

func TestListenForStatusToFetch(t *testing.T) {
	// listening for status requests
	ts, err := utiltest.ServeFile("./TestData/realm-status.json")
	if !assert.Nil(t, err) {
		return
	}

	// loading state with a resolver to the test server and a single region
	reg := region{Name: "test", Hostname: "test"}
	sta := state{
		resolver: resolver{
			getStatusURL: func(regionHostname string) string { return ts.URL },
		},
		regions: []region{reg},
	}

	// connecting
	mess, err := newMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}
	sta.messenger = mess
	sta.statuses = map[regionName]status{}

	// setting up a listener for responding to status requests
	stop := make(chan interface{})
	err = sta.listenForStatus(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive statuses
	stat, err := newStatusFromMessenger(reg, mess)
	if !assert.Nil(t, err) {
		stop <- struct{}{}

		return
	}

	if !assert.True(t, len(stat.Realms) > 0) {
		stop <- struct{}{}

		return
	}

	// flagging the status listener to exit
	stop <- struct{}{}
}
