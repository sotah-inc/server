package internal

import (
	"testing"

	"github.com/sotah-inc/server/app/sortdirections"
	"github.com/sotah-inc/server/app/sortkinds"

	"github.com/stretchr/testify/assert"
)

func TestMiniAuctionsSortByItem(t *testing.T) {
	t.Skip("TODO after creating blizzard package")

	mad, err := newMiniAuctionsDataFromFilepath("./TestData/mini-auctions.json")
	if !assert.Nil(t, err) {
		return
	}

	sorter := newMiniAuctionSorter()
	err = sorter.sort(sortkinds.Item, sortdirections.Up, mad.Auctions)
	if !assert.Nil(t, err) {
		return
	}

	expectedSortedMad, err := newMiniAuctionsDataFromFilepath("./TestData/mini-auctions-sort-by-item.json")
	if !assert.Nil(t, err) {
		return
	}

	if !assert.Equal(t, expectedSortedMad.Auctions, mad.Auctions) {
		return
	}
}

func TestMiniAuctionsSortByItemReversed(t *testing.T) {
	t.Skip("TODO after creating blizzard package")

	mad, err := newMiniAuctionsDataFromFilepath("./TestData/mini-auctions.json")
	if !assert.Nil(t, err) {
		return
	}

	sorter := newMiniAuctionSorter()
	err = sorter.sort(sortkinds.Item, sortdirections.Down, mad.Auctions)
	if !assert.Nil(t, err) {
		return
	}

	expectedSortedMad, err := newMiniAuctionsDataFromFilepath("./TestData/mini-auctions-sort-by-item-reversed.json")
	if !assert.Nil(t, err) {
		return
	}

	if !assert.Equal(t, expectedSortedMad.Auctions, mad.Auctions) {
		return
	}
}
