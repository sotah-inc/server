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

	sortedMad := mad.Auctions.sortByItem()
	expectedSortedMad, err := newMiniAuctionsDataFromFilepath("./TestData/mini-auctions-sort-by-item.json")
	if !assert.Nil(t, err) {
		return
	}

	if !assert.Equal(t, expectedSortedMad.Auctions, sortedMad) {
		return
	}
}
