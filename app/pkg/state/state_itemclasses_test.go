package state

import (
	"testing"

	"github.com/sotah-inc/server/app/blizzard"
	"github.com/stretchr/testify/assert"
)

func TestListenForItemClasses(t *testing.T) {
	sta := State{}

	// connecting
	mess, err := newMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}
	sta.Messenger = mess

	// resolving item-classes
	result, err := blizzard.NewItemClassesFromFilepath("./TestData/item-classes.json")
	if !assert.Nil(t, err) {
		return
	}

	// attaching the items to the State
	sta.itemClasses = result

	// setting up a subscriber that will publish items
	stop := make(chan interface{})
	err = sta.listenForItemClasses(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive items
	receivedItemClasses, err := newItemClassesFromMessenger(mess)
	if !assert.Nil(t, err) {
		stop <- struct{}{}

		return
	}
	if !assert.Equal(t, result.Classes, receivedItemClasses.Classes) {
		stop <- struct{}{}

		return
	}

	stop <- struct{}{}
}
