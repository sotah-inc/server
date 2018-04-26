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
	sortedMad := sorter["item"](mad.Auctions)
	expectedSortedMad, err := newMiniAuctionsDataFromFilepath("./TestData/mini-auctions-sort-by-item.json")
	if !assert.Nil(t, err) {
		return
	}

	if !assert.Equal(t, expectedSortedMad.Auctions, sortedMad) {
		return
	}
}

func TestMiniAuctionsSortByItemReversed(t *testing.T) {
	mad, err := newMiniAuctionsDataFromFilepath("./TestData/mini-auctions.json")
	if !assert.Nil(t, err) {
		return
	}

	sorter := newMiniAuctionSorter()
	sortedMad := sorter["item-r"](mad.Auctions)
	expectedSortedMad, err := newMiniAuctionsDataFromFilepath("./TestData/mini-auctions-sort-by-item-reversed.json")
	if !assert.Nil(t, err) {
		return
	}

	if !assert.Equal(t, expectedSortedMad.Auctions, sortedMad) {
		return
	}
}
