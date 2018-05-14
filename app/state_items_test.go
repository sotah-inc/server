package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestListenForItems(t *testing.T) {
	sta := state{}

	// connecting
	mess, err := newMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}
	sta.messenger = mess

	// resolving items
	result, err := newItemListResultFromFilepath("./TestData/item-list-result.json")
	if !assert.Nil(t, err) {
		return
	}
	expectedResult, err := newItemListResultFromFilepath("./TestData/item-list-result-sorted.json")
	if !assert.Nil(t, err) {
		return
	}

	// attaching the items to the state
	sta.items = map[itemID]item{}
	for _, resultItem := range result.Items {
		sta.items[resultItem.ID] = resultItem
	}

	// setting up a subscriber that will publish items
	stop := make(chan interface{})
	err = sta.listenForItems(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive items
	receivedItemList, err := newItemListResultFromMessenger(mess, itemsRequest{Query: ""})
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
