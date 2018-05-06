package main

import (
	"errors"
	"fmt"
	"sort"

	"github.com/ihsw/sotah-server/app/sortdirections"
	"github.com/ihsw/sotah-server/app/sortkinds"
	log "github.com/sirupsen/logrus"
)

type miniAuctionSortFn func(miniAuctionList) miniAuctionList

func newMiniAuctionSorter() miniAuctionSorter {
	return miniAuctionSorter{
		"item": func(mAuctionList miniAuctionList) miniAuctionList {
			log.WithField("sort-kind", "item").Info("Sorting")
			sort.Sort(byItem(mAuctionList))

			return mAuctionList
		},
		"item-r": func(mAuctionList miniAuctionList) miniAuctionList {
			log.WithField("sort-kind", "item-r").Info("Sorting")
			sort.Sort(byItemReversed(mAuctionList))

			return mAuctionList
		},
		"quantity": func(mAuctionList miniAuctionList) miniAuctionList {
			log.WithField("sort-kind", "quantity").Info("Sorting")
			sort.Sort(byQuantity(mAuctionList))

			return mAuctionList
		},
		"quantity-r": func(mAuctionList miniAuctionList) miniAuctionList {
			log.WithField("sort-kind", "quantity-r").Info("Sorting")
			sort.Sort(byQuantityReversed(mAuctionList))

			return mAuctionList
		},
	}
}

type miniAuctionSorter map[string]miniAuctionSortFn

func (mas miniAuctionSorter) sort(kind sortkinds.SortKind, direction sortdirections.SortDirection, data miniAuctionList) (miniAuctionList, error) {
	// resolving the sort kind as a string
	kindMap := map[sortkinds.SortKind]string{
		sortkinds.Item:     "item",
		sortkinds.Quantity: "quantity",
	}
	resolvedKind, ok := kindMap[kind]
	if !ok {
		return miniAuctionList{}, errors.New("Invalid sort kind")
	}

	if direction == sortdirections.Down {
		resolvedKind = fmt.Sprintf("%s-r", resolvedKind)
	}

	// resolving the sort func
	sortFn, ok := mas[resolvedKind]
	if !ok {
		return miniAuctionList{}, errors.New("Sorter not found")
	}

	return sortFn(data), nil
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
