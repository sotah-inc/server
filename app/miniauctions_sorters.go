package main

import "sort"

type miniAuctionSortFn func(miniAuctionList) miniAuctionList

type miniAuctionSorter map[string]miniAuctionSortFn

func getMiniAuctionSorter() miniAuctionSorter {
	return miniAuctionSorter{
		"item": func(mAuctionList miniAuctionList) miniAuctionList {
			sort.Sort(byItem(mAuctionList))
			return mAuctionList
		},
	}
}

type byItem miniAuctionList

func (bi byItem) Len() int           { return len(bi) }
func (bi byItem) Swap(i, j int)      { bi[i], bi[j] = bi[j], bi[i] }
func (bi byItem) Less(i, j int) bool { return bi[i].Item < bi[j].Item }
