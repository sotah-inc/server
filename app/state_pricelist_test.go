package main

import (
	"testing"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/stretchr/testify/assert"
)

func TestListenForPricelist(t *testing.T) {
	sta := state{}

	// connecting
	mess, err := newMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}
	sta.messenger = mess

	// building test auctions
	aucs, err := blizzard.NewAuctionsFromFilepath("./TestData/auctions.json")
	if !assert.Nil(t, err) {
		return
	}
	maList := newMiniAuctionListFromBlizzardAuctions(aucs.Auctions)

	// building a test realm
	reg := region{Name: "us"}
	rea, err := blizzard.NewRealmFromFilepath("./TestData/realm.json")
	if !assert.Nil(t, err) {
		return
	}

	// attaching the auctions to the state
	sta.auctions = map[regionName]map[blizzard.RealmSlug]miniAuctionList{
		reg.Name: map[blizzard.RealmSlug]miniAuctionList{
			rea.Slug: maList,
		},
	}

	// setting up a subscriber that will publish pricelists
	stop := make(chan interface{})
	err = sta.listenForPriceList(stop)
	if !assert.Nil(t, err) {
		return
	}

	receivedPriceList, err := newPriceListResponseFromMessenger(priceListRequest{
		RegionName: reg.Name,
		RealmSlug:  rea.Slug,
		ItemIds:    maList.itemIds(),
	}, mess)
	if !assert.Nil(t, err) {
		stop <- struct{}{}

		return
	}
	if !assert.NotZero(t, len(receivedPriceList.PriceList)) {
		stop <- struct{}{}

		return
	}

	stop <- struct{}{}
}
