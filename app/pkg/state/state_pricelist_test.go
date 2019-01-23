package state

import (
	"testing"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/stretchr/testify/assert"
)

func TestListenForPricelist(t *testing.T) {
	sta := State{}

	// connecting
	mess, err := newMessengerFromEnvVars("NATS_HOST", "NATS_PORT")
	if !assert.Nil(t, err) {
		return
	}
	sta.Messenger = mess

	// building test auctions
	aucs, err := blizzard.NewAuctionsFromFilepath("./TestData/auctions.json")
	if !assert.Nil(t, err) {
		return
	}
	maList := newMiniAuctionListFromBlizzardAuctions(aucs.Auctions)

	// building a test Realm
	reg := region{Name: "us"}
	rea, err := blizzard.NewRealmFromFilepath("./TestData/Realm.json")
	if !assert.Nil(t, err) {
		return
	}

	// setting up a subscriber that will publish pricelists
	stop := make(chan interface{})
	err = sta.ListenForPriceList(stop)
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
