package main

import (
	"testing"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/stretchr/testify/assert"
)

func TestListenForAuctionsQuery(t *testing.T) {
	t.Skip("TODO after creating blizzard package")

	sta := state{}

	// connecting
	mess, err := newMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}
	sta.messenger = mess

	// resolving auctions and attaching them to the state
	maData, err := newMiniAuctionsDataFromFilepath("./TestData/mini-auctions.json")
	if !assert.Nil(t, err) {
		return
	}
	reg := region{Name: "test"}
	rea := realm{Realm: blizzard.Realm{Slug: "test"}, region: reg}
	sta.auctions = map[regionName]map[blizzard.RealmSlug]miniAuctionList{
		reg.Name: map[blizzard.RealmSlug]miniAuctionList{rea.Slug: maData.Auctions},
	}

	// resolving items and attaching them to the state
	ilResult, err := newItemsQueryResultFromFilepath("./TestData/item-list-result.json")
	if !assert.Nil(t, err) {
		return
	}
	sta.items = map[blizzard.ItemID]item{}
	for _, resultItem := range ilResult.Items {
		sta.items[resultItem.Item.ID] = resultItem.Item
	}

	// setting up a subscriber that will publish items
	stop := make(chan interface{})
	err = sta.listenForAuctionsQuery(stop)
	if !assert.Nil(t, err) {
		return
	}

	// subscribing to receive auctions-query results
	aqResult, err := newAuctionsQueryResultFromMessenger(
		mess,
		auctionsQueryRequest{Query: "", RegionName: reg.Name, RealmSlug: rea.Slug},
	)
	if !assert.Nil(t, err) {
		stop <- struct{}{}

		return
	}
	if !assert.NotZero(t, len(aqResult.Items)) {
		stop <- struct{}{}

		return
	}

	stop <- struct{}{}
}
