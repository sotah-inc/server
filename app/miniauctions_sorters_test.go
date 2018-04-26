package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMiniAuctionsSortByItem(t *testing.T) {
	mad, err := newMiniAuctionsDataFromFilepath("./TestData/mini-auctions.json")
	if !assert.Nil(t, err) {
		return
	}

	sorter := newMiniAuctionSorter()
	sortedData, err := sorter.sort("item", mad.Auctions)
	if !assert.Nil(t, err) {
		return
	}

	expectedSortedMad, err := newMiniAuctionsDataFromFilepath("./TestData/mini-auctions-sort-by-item.json")
	if !assert.Nil(t, err) {
		return
	}

	if !assert.Equal(t, expectedSortedMad.Auctions, sortedData) {
		return
	}
}

func TestMiniAuctionsSortByItemReversed(t *testing.T) {
	mad, err := newMiniAuctionsDataFromFilepath("./TestData/mini-auctions.json")
	if !assert.Nil(t, err) {
		return
	}

	sorter := newMiniAuctionSorter()
	sortedData, err := sorter.sort("item-r", mad.Auctions)
	expectedSortedMad, err := newMiniAuctionsDataFromFilepath("./TestData/mini-auctions-sort-by-item-reversed.json")
	if !assert.Nil(t, err) {
		return
	}

	if !assert.Equal(t, expectedSortedMad.Auctions, sortedData) {
		return
	}
}
