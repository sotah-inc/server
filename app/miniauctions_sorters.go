package main

import "sort"

// ByItem - sorting an auction list by item
type ByItem miniAuctionList

// Len - sort interface func
func (bi ByItem) Len() int { return len(bi) }

// Swap - sort interface func
func (bi ByItem) Swap(i, j int) { bi[i], bi[j] = bi[j], bi[i] }

// Less - sort interface func
func (bi ByItem) Less(i, j int) bool { return bi[i].Item < bi[j].Item }

func (mAuctionList miniAuctionList) sortByItem() miniAuctionList {
	sort.Sort(ByItem(mAuctionList))
	return mAuctionList
}
