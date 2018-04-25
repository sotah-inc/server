package main

import "sort"

type miniAuctionSortFn func(miniAuctionList) miniAuctionList

type miniAuctionSorter map[string]miniAuctionSortFn

func getMiniAuctionSorter() miniAuctionSorter {
	return miniAuctionSorter{
		"item": func(mAuctionList miniAuctionList) miniAuctionList {
			sort.Sort(ByItem(mAuctionList))
			return mAuctionList
		},
	}
}

// ByItem - sorting an auction list by item
type ByItem miniAuctionList

func (bi ByItem) Len() int           { return len(bi) }
func (bi ByItem) Swap(i, j int)      { bi[i], bi[j] = bi[j], bi[i] }
func (bi ByItem) Less(i, j int) bool { return bi[i].Item < bi[j].Item }
