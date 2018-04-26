package main

import (
	"errors"
	"sort"
)

type miniAuctionSortFn func(miniAuctionList) miniAuctionList

func newMiniAuctionSorter() miniAuctionSorter {
	return miniAuctionSorter{
		"item": func(mAuctionList miniAuctionList) miniAuctionList {
			sort.Sort(byItem(mAuctionList))
			return mAuctionList
		},
		"item-r": func(mAuctionList miniAuctionList) miniAuctionList {
			sort.Sort(byItemReversed(mAuctionList))
			return mAuctionList
		},
	}
}

type miniAuctionSorter map[string]miniAuctionSortFn

func (mas miniAuctionSorter) sort(sorterName string, data miniAuctionList) (miniAuctionList, error) {
	sortFn, ok := mas[sorterName]
	if !ok {
		return miniAuctionList{}, errors.New("Sorter not found")
	}

	return sortFn(data), nil
}

type byItem miniAuctionList

func (bi byItem) Len() int           { return len(bi) }
func (bi byItem) Swap(i, j int)      { bi[i], bi[j] = bi[j], bi[i] }
func (bi byItem) Less(i, j int) bool { return bi[i].Item < bi[j].Item }

type byItemReversed miniAuctionList

func (bi byItemReversed) Len() int           { return len(bi) }
func (bi byItemReversed) Swap(i, j int)      { bi[i], bi[j] = bi[j], bi[i] }
func (bi byItemReversed) Less(i, j int) bool { return bi[i].Item > bi[j].Item }
