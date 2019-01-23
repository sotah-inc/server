package internal

import (
	"errors"
	"fmt"
	"sort"

	"github.com/sotah-inc/server/app/pkg/state/sortdirections"
	"github.com/sotah-inc/server/app/pkg/state/sortkinds"
)

type miniAuctionSortFn func(MiniAuctionList)

func newMiniAuctionSorter() miniAuctionSorter {
	return miniAuctionSorter{
		"Item":         func(mAuctionList MiniAuctionList) { sort.Sort(byItem(mAuctionList)) },
		"Item-r":       func(mAuctionList MiniAuctionList) { sort.Sort(byItemReversed(mAuctionList)) },
		"quantity":     func(mAuctionList MiniAuctionList) { sort.Sort(byQuantity(mAuctionList)) },
		"quantity-r":   func(mAuctionList MiniAuctionList) { sort.Sort(byQuantityReversed(mAuctionList)) },
		"bid":          func(mAuctionList MiniAuctionList) { sort.Sort(byBid(mAuctionList)) },
		"bid-r":        func(mAuctionList MiniAuctionList) { sort.Sort(byBidReversed(mAuctionList)) },
		"buyout":       func(mAuctionList MiniAuctionList) { sort.Sort(byBuyout(mAuctionList)) },
		"buyout-r":     func(mAuctionList MiniAuctionList) { sort.Sort(byBuyoutReversed(mAuctionList)) },
		"buyout_per":   func(mAuctionList MiniAuctionList) { sort.Sort(byBuyoutPer(mAuctionList)) },
		"buyout_per-r": func(mAuctionList MiniAuctionList) { sort.Sort(byBuyoutPerReversed(mAuctionList)) },
		"Auctions":     func(mAuctionList MiniAuctionList) { sort.Sort(byAuctions(mAuctionList)) },
		"Auctions-r":   func(mAuctionList MiniAuctionList) { sort.Sort(byAuctionsReversed(mAuctionList)) },
		"Owner":        func(mAuctionList MiniAuctionList) { sort.Sort(byOwner(mAuctionList)) },
		"Owner-r":      func(mAuctionList MiniAuctionList) { sort.Sort(byOwnerReversed(mAuctionList)) },
	}
}

type miniAuctionSorter map[string]miniAuctionSortFn

func (mas miniAuctionSorter) sort(kind sortkinds.SortKind, direction sortdirections.SortDirection, data MiniAuctionList) error {
	// resolving the sort kind as a string
	kindMap := map[sortkinds.SortKind]string{
		sortkinds.Item:      "Item",
		sortkinds.Quantity:  "quantity",
		sortkinds.Bid:       "bid",
		sortkinds.Buyout:    "buyout",
		sortkinds.BuyoutPer: "buyout_per",
		sortkinds.Auctions:  "Auctions",
		sortkinds.Owner:     "Owner",
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

type byItem MiniAuctionList

func (by byItem) Len() int           { return len(by) }
func (by byItem) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byItem) Less(i, j int) bool { return by[i].ItemID < by[j].ItemID }

type byItemReversed MiniAuctionList

func (by byItemReversed) Len() int           { return len(by) }
func (by byItemReversed) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byItemReversed) Less(i, j int) bool { return by[i].ItemID > by[j].ItemID }

type byQuantity MiniAuctionList

func (by byQuantity) Len() int           { return len(by) }
func (by byQuantity) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byQuantity) Less(i, j int) bool { return by[i].Quantity < by[j].Quantity }

type byQuantityReversed MiniAuctionList

func (by byQuantityReversed) Len() int           { return len(by) }
func (by byQuantityReversed) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byQuantityReversed) Less(i, j int) bool { return by[i].Quantity > by[j].Quantity }

type byBid MiniAuctionList

func (by byBid) Len() int           { return len(by) }
func (by byBid) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byBid) Less(i, j int) bool { return by[i].Bid < by[j].Bid }

type byBidReversed MiniAuctionList

func (by byBidReversed) Len() int           { return len(by) }
func (by byBidReversed) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byBidReversed) Less(i, j int) bool { return by[i].Bid > by[j].Bid }

type byBuyout MiniAuctionList

func (by byBuyout) Len() int           { return len(by) }
func (by byBuyout) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byBuyout) Less(i, j int) bool { return by[i].Buyout < by[j].Buyout }

type byBuyoutReversed MiniAuctionList

func (by byBuyoutReversed) Len() int           { return len(by) }
func (by byBuyoutReversed) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byBuyoutReversed) Less(i, j int) bool { return by[i].Buyout > by[j].Buyout }

type byBuyoutPer MiniAuctionList

func (by byBuyoutPer) Len() int           { return len(by) }
func (by byBuyoutPer) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byBuyoutPer) Less(i, j int) bool { return by[i].BuyoutPer < by[j].BuyoutPer }

type byBuyoutPerReversed MiniAuctionList

func (by byBuyoutPerReversed) Len() int           { return len(by) }
func (by byBuyoutPerReversed) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byBuyoutPerReversed) Less(i, j int) bool { return by[i].BuyoutPer > by[j].BuyoutPer }

type byAuctions MiniAuctionList

func (by byAuctions) Len() int           { return len(by) }
func (by byAuctions) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byAuctions) Less(i, j int) bool { return len(by[i].AucList) < len(by[j].AucList) }

type byAuctionsReversed MiniAuctionList

func (by byAuctionsReversed) Len() int           { return len(by) }
func (by byAuctionsReversed) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byAuctionsReversed) Less(i, j int) bool { return len(by[i].AucList) > len(by[j].AucList) }

type byOwner MiniAuctionList

func (by byOwner) Len() int           { return len(by) }
func (by byOwner) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byOwner) Less(i, j int) bool { return by[i].Owner < by[j].Owner }

type byOwnerReversed MiniAuctionList

func (by byOwnerReversed) Len() int           { return len(by) }
func (by byOwnerReversed) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by byOwnerReversed) Less(i, j int) bool { return by[i].Owner > by[j].Owner }
