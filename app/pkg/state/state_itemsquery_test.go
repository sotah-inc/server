package state

import (
	"testing"

	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/stretchr/testify/assert"
)

func TestListenForItems(t *testing.T) {
	sta := State{}

	// connecting
	mess, err := messenger.NewMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}

	// resolving items
	_, err = newItemsQueryResultFromFilepath("./TestData/item-list-result.json")
	if !assert.Nil(t, err) {
		return
	}
	expectedResult, err := newItemsQueryResultFromFilepath("./TestData/item-list-result-sorted.json")
	if !assert.Nil(t, err) {
		return
	}

	// setting up a subscriber that will publish items
	stop := make(chan interface{})
	err = sta.ListenForItemsQuery(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive items
	receivedItemList, err := newItemsQueryResultFromMessenger(mess, itemsQueryRequest{Query: ""})
	if !assert.Nil(t, err) {
		stop <- struct{}{}

		return
	}
	if !assert.NotZero(t, len(receivedItemList.Items)) {
		stop <- struct{}{}

		return
	}
	if !assert.Equal(t, expectedResult.Items, receivedItemList.Items) {
		stop <- struct{}{}

		return
	}

	stop <- struct{}{}
}

func TestListenForItemsFiltered(t *testing.T) {
	sta := State{}

	// connecting
	mess, err := messenger.NewMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}

	// resolving items
	_, err = newItemsQueryResultFromFilepath("./TestData/item-list-result.json")
	if !assert.Nil(t, err) {
		return
	}
	expectedResult, err := newItemsQueryResultFromFilepath("./TestData/item-list-result-filtered.json")
	if !assert.Nil(t, err) {
		return
	}

	// setting up a subscriber that will publish items
	stop := make(chan interface{})
	err = sta.ListenForItemsQuery(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive items
	receivedItemList, err := newItemsQueryResultFromMessenger(mess, itemsQueryRequest{Query: "Axe"})
	if !assert.Nil(t, err) {
		stop <- struct{}{}

		return
	}
	if !assert.NotZero(t, len(receivedItemList.Items)) {
		stop <- struct{}{}

		return
	}
	if !assert.Equal(t, expectedResult.Items, receivedItemList.Items) {
		stop <- struct{}{}

		return
	}

	stop <- struct{}{}
}
