package main

import (
	"errors"
	"fmt"
	"sort"

	"github.com/ihsw/sotah-server/app/sortdirections"
	"github.com/ihsw/sotah-server/app/sortkinds"
)

type miniAuctionSortFn func(miniAuctionList)

func newMiniAuctionSorter() miniAuctionSorter {
	return miniAuctionSorter{
		"item":       func(mAuctionList miniAuctionList) { sort.Sort(byItem(mAuctionList)) },
		"item-r":     func(mAuctionList miniAuctionList) { sort.Sort(byItemReversed(mAuctionList)) },
		"quantity":   func(mAuctionList miniAuctionList) { sort.Sort(byQuantity(mAuctionList)) },
		"quantity-r": func(mAuctionList miniAuctionList) { sort.Sort(byQuantityReversed(mAuctionList)) },
		"bid":        func(mAuctionList miniAuctionList) { sort.Sort(byBid(mAuctionList)) },
		"bid-r":      func(mAuctionList miniAuctionList) { sort.Sort(byBidReversed(mAuctionList)) },
		"buyout":     func(mAuctionList miniAuctionList) { sort.Sort(byBuyout(mAuctionList)) },
		"buyout-r":   func(mAuctionList miniAuctionList) { sort.Sort(byBuyoutReversed(mAuctionList)) },
		"auctions":   func(mAuctionList miniAuctionList) { sort.Sort(byAuctions(mAuctionList)) },
		"auctions-r": func(mAuctionList miniAuctionList) { sort.Sort(byAuctionsReversed(mAuctionList)) },
		"owner":      func(mAuctionList miniAuctionList) { sort.Sort(byOwner(mAuctionList)) },
		"owner-r":    func(mAuctionList miniAuctionList) { sort.Sort(byOwnerReversed(mAuctionList)) },
	}
}

type miniAuctionSorter map[string]miniAuctionSortFn

func (mas miniAuctionSorter) sort(kind sortkinds.SortKind, direction sortdirections.SortDirection, data miniAuctionList) error {
	// resolving the sort kind as a string
	kindMap := map[sortkinds.SortKind]string{
		sortkinds.Item:     "item",
		sortkinds.Quantity: "quantity",
		sortkinds.Bid:      "bid",
		sortkinds.Buyout:   "buyout",
		sortkinds.Auctions: "auctions",
		sortkinds.Owner:    "owner",
	}
	resolvedKind, ok := kindMap[kind]
	if !ok {
		return errors.New("Invalid sort kind")
	}

	if direction == sortdirections.Down {
		resolvedKind = fmt.Sprintf("%s-r", resolvedKind)
	}

	// resolving the sort func
	sortFn, ok := mas[resolvedKind]
	if !ok {
		return errors.New("Sorter not found")
	}

	sortFn(data)

	return nil
}

type byItem miniAuctionList

func (by byItem) Len() int           { return len(by) }
func (by byItem) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byItem) Less(i, j int) bool { return by[i].Item < by[j].Item }

type byItemReversed miniAuctionList

func (by byItemReversed) Len() int           { return len(by) }
func (by byItemReversed) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byItemReversed) Less(i, j int) bool { return by[i].Item > by[j].Item }

type byQuantity miniAuctionList

func (by byQuantity) Len() int           { return len(by) }
func (by byQuantity) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byQuantity) Less(i, j int) bool { return by[i].Quantity < by[j].Quantity }

type byQuantityReversed miniAuctionList

func (by byQuantityReversed) Len() int           { return len(by) }
func (by byQuantityReversed) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byQuantityReversed) Less(i, j int) bool { return by[i].Quantity > by[j].Quantity }

type byBid miniAuctionList

func (by byBid) Len() int           { return len(by) }
func (by byBid) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byBid) Less(i, j int) bool { return by[i].Bid < by[j].Bid }

type byBidReversed miniAuctionList

func (by byBidReversed) Len() int           { return len(by) }
func (by byBidReversed) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byBidReversed) Less(i, j int) bool { return by[i].Bid > by[j].Bid }

type byBuyout miniAuctionList

func (by byBuyout) Len() int           { return len(by) }
func (by byBuyout) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byBuyout) Less(i, j int) bool { return by[i].Buyout < by[j].Buyout }

type byBuyoutReversed miniAuctionList

func (by byBuyoutReversed) Len() int           { return len(by) }
func (by byBuyoutReversed) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byBuyoutReversed) Less(i, j int) bool { return by[i].Buyout > by[j].Buyout }

type byAuctions miniAuctionList

func (by byAuctions) Len() int           { return len(by) }
func (by byAuctions) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byAuctions) Less(i, j int) bool { return len(by[i].AucList) < len(by[j].AucList) }

type byAuctionsReversed miniAuctionList

func (by byAuctionsReversed) Len() int           { return len(by) }
func (by byAuctionsReversed) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byAuctionsReversed) Less(i, j int) bool { return len(by[i].AucList) > len(by[j].AucList) }

type byOwner miniAuctionList

func (by byOwner) Len() int           { return len(by) }
func (by byOwner) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byOwner) Less(i, j int) bool { return by[i].Owner < by[j].Owner }

type byOwnerReversed miniAuctionList

func (by byOwnerReversed) Len() int           { return len(by) }
func (by byOwnerReversed) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byOwnerReversed) Less(i, j int) bool { return by[i].Owner > by[j].Owner }
