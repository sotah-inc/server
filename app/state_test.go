package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/utiltest"

	"github.com/ihsw/sotah-server/app/subjects"

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
	sta.statuses = map[regionName]*status{reg.Name: s}

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
	sta.statuses = map[regionName]*status{}

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
		resolver: &resolver{
			getStatusURL: func(regionHostname string) string { return ts.URL },
		},
	}

	// connecting
	mess, err := newMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}
	sta.messenger = mess
	sta.statuses = map[regionName]*status{}

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
		resolver: &resolver{
			getStatusURL: func(regionHostname string) string { return ts.URL },
		},
		regions:  []region{reg},
		auctions: map[regionName]map[realmSlug]miniAuctionList{},
	}
	sta.auctions[reg.Name] = map[realmSlug]miniAuctionList{}

	// connecting
	mess, err := newMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}
	sta.messenger = mess
	sta.statuses = map[regionName]*status{}

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
	sta.statuses = map[regionName]*status{reg.Name: s}

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

func TestListenForOwners(t *testing.T) {
	sta := state{}

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

	// fetching expected owners
	expectedOwners, err := newOwnersFromFilepath("./TestData/owners.json")
	if !assert.Nil(t, err) {
		return
	}

	// attaching the auctions to the state
	sta.auctions = map[regionName]map[realmSlug]miniAuctionList{
		reg.Name: {
			rea.Slug: a.Auctions.minimize(),
		},
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

	// fetching expected owners
	expectedOwners, err := newOwnersFromFilepath("./TestData/owners-filtered.json")
	if !assert.Nil(t, err) {
		return
	}

	// attaching the auctions to the state
	sta.auctions = map[regionName]map[realmSlug]miniAuctionList{
		reg.Name: {
			rea.Slug: a.Auctions.minimize(),
		},
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
