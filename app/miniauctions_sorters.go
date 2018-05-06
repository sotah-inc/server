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
		"item": func(mAuctionList miniAuctionList) {
			sort.Sort(byItem(mAuctionList))
		},
		"item-r": func(mAuctionList miniAuctionList) {
			sort.Sort(byItemReversed(mAuctionList))
		},
		"quantity": func(mAuctionList miniAuctionList) {
			sort.Sort(byQuantity(mAuctionList))
		},
		"quantity-r": func(mAuctionList miniAuctionList) {
			sort.Sort(byQuantityReversed(mAuctionList))
		},
	}
}

type miniAuctionSorter map[string]miniAuctionSortFn

func (mas miniAuctionSorter) sort(kind sortkinds.SortKind, direction sortdirections.SortDirection, data miniAuctionList) error {
	// resolving the sort kind as a string
	kindMap := map[sortkinds.SortKind]string{
		sortkinds.Item:     "item",
		sortkinds.Quantity: "quantity",
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
