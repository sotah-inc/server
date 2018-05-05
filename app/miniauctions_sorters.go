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
	}
}

type miniAuctionSorter map[string]miniAuctionSortFn

func (mas miniAuctionSorter) sort(kind sortkinds.SortKind, direction sortdirections.SortDirection, data miniAuctionList) (miniAuctionList, error) {
	// resolving the sort kind as a string
	kindMap := map[sortkinds.SortKind]string{
		sortkinds.Item: "item",
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

func (bi byItem) Len() int           { return len(bi) }
func (bi byItem) Swap(i, j int)      { bi[i], bi[j] = bi[j], bi[i] }
func (bi byItem) Less(i, j int) bool { return bi[i].Item < bi[j].Item }

type byItemReversed miniAuctionList

func (bi byItemReversed) Len() int           { return len(bi) }
func (bi byItemReversed) Swap(i, j int)      { bi[i], bi[j] = bi[j], bi[i] }
func (bi byItemReversed) Less(i, j int) bool { return bi[i].Item > bi[j].Item }
