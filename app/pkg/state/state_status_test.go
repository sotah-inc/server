package state

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sotah-inc/server/app/internal"
	"github.com/sotah-inc/server/app/pkg/messenger"

	"github.com/sotah-inc/server/app/pkg/utiltest"
	"github.com/stretchr/testify/assert"
)

func TestListenForStatus(t *testing.T) {
	sta := State{}

	// connecting
	mess, err := messenger.NewMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}

	// building test status
	reg := internal.Region{Name: "test", Hostname: "test"}
	s, err := internal.NewStatusFromFilepath(reg, "./TestData/Realm-status.json")
	if !assert.Nil(t, err) {
		return
	}
	if !utiltest.ValidateStatus(t, reg, s) {
		return
	}
	sta.Regions = []internal.Region{reg}
	sta.Statuses = map[internal.RegionName]internal.Status{reg.Name: s}

	// setting up a listener for responding to status requests
	stop := make(chan interface{})
	err = sta.ListenForStatus(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive Statuses
	receivedStatus, err := internal.NewStatusFromMessenger(reg, mess)
	if !assert.Nil(t, err) || !assert.Equal(t, s.Region.Hostname, receivedStatus.Region.Hostname) {
		stop <- struct{}{}

		return
	}

	// flagging the status listener to exit
	stop <- struct{}{}
}

func TestListenForNonexistentStatusNoResolver(t *testing.T) {
	sta := State{}

	// connecting
	mess, err := messenger.NewMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}
	sta.Statuses = map[internal.RegionName]internal.Status{}

	// setting up a listener for responding to status requests
	stop := make(chan interface{})
	err = sta.ListenForStatus(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive Statuses
	_, err = internal.NewStatusFromMessenger(internal.Region{Name: "test", Hostname: "test"}, mess)
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

	sta := State{
		Resolver: internal.Resolver{
			GetStatusURL: func(regionHostname string) string { return ts.URL },
		},
	}

	// connecting
	mess, err := messenger.NewMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}
	sta.Statuses = map[internal.RegionName]internal.Status{}

	// setting up a listener for responding to status requests
	stop := make(chan interface{})
	err = sta.ListenForStatus(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive Statuses
	_, err = internal.NewStatusFromMessenger(internal.Region{Name: "test", Hostname: "test"}, mess)
	if !assert.NotNil(t, err) && assert.Equal(t, "Invalid region", err.Error()) {
		stop <- struct{}{}

		return
	}

	// flagging the status listener to exit
	stop <- struct{}{}
}

func TestListenForStatusToFetch(t *testing.T) {
	// listening for status requests
	ts, err := utiltest.ServeFile("./TestData/Realm-status.json")
	if !assert.Nil(t, err) {
		return
	}

	// loading State with a Resolver to the test server and a single region
	reg := internal.Region{Name: "test", Hostname: "test"}
	sta := State{
		Resolver: internal.Resolver{
			GetStatusURL: func(regionHostname string) string { return ts.URL },
		},
		Regions: []internal.Region{reg},
	}

	// connecting
	mess, err := messenger.NewMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}

	sta.Statuses = map[internal.RegionName]internal.Status{}

	// setting up a listener for responding to status requests
	stop := make(chan interface{})
	err = sta.ListenForStatus(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive Statuses
	stat, err := internal.NewStatusFromMessenger(reg, mess)
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
