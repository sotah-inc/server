package internal

import (
	"testing"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/stretchr/testify/assert"
)

func TestNewMiniAuctionsDataFromFilepath(t *testing.T) {
	t.Skip("TODO after creating blizzard package")

	mad, err := newMiniAuctionsDataFromFilepath("./TestData/mini-Auctions.json")
	if !assert.Nil(t, err) {
		return
	}

	if !assert.Equal(t, 3, len(mad.Auctions)) {
		return
	}
}

func TestNewMiniAuctionsFromMessenger(t *testing.T) {
	sta := state.State{}

	// connecting
	mess, err := messenger.NewMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}
	sta.Messenger = mess

	// building test Auctions
	aucs, err := blizzard.NewAuctionsFromFilepath("./TestData/Auctions.json")
	if !assert.Nil(t, err) {
		return
	}

	// building a test Realm
	reg := Region{Name: "us"}
	rea, err := blizzard.NewRealmFromFilepath("./TestData/Realm.json")
	if !assert.Nil(t, err) {
		return
	}

	// setting up a subscriber that will publish Auctions
	stop := make(chan interface{})
	err = sta.ListenForAuctions(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive Auctions
	receivedMiniAuctions, err := newMiniAuctionsListFromMessenger(newMiniAuctionsListFromMessengerConfig{
		realm:     Realm{Realm: rea, Region: reg},
		messenger: mess,
		count:     10,
	})
	if !assert.Nil(t, err) {
		stop <- struct{}{}

		return
	}
	if !assert.NotZero(t, len(receivedMiniAuctions)) {
		stop <- struct{}{}

		return
	}
	if !assert.Equal(t, len(aucs.Auctions), len(receivedMiniAuctions)) {
		stop <- struct{}{}

		return
	}

	// flagging the status listener to exit
	stop <- struct{}{}
}
