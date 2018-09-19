package main

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/sortdirections"
	"github.com/ihsw/sotah-server/app/sortkinds"

	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
	"github.com/ihsw/sotah-server/app/util"
)

func newMiniAuctionsDataFromFilepath(relativeFilepath string) (miniAuctionsData, error) {
	body, err := util.ReadFile(relativeFilepath)
	if err != nil {
		return miniAuctionsData{}, err
	}

	return newMiniAuctionsData(body)
}

func newMiniAuctionsData(body []byte) (miniAuctionsData, error) {
	mad := &miniAuctionsData{}
	if err := json.Unmarshal(body, mad); err != nil {
		return miniAuctionsData{}, err
	}

	return *mad, nil
}

type miniAuctionsData struct {
	Auctions miniAuctionList `json:"auctions"`
}

type newMiniAuctionsListFromMessengerConfig struct {
	realm         realm
	messenger     messenger
	count         int
	page          int
	sortDirection sortdirections.SortDirection
	sortKind      sortkinds.SortKind
	ownerFilter   ownerName
}

func (config newMiniAuctionsListFromMessengerConfig) toAuctionsRequest() auctionsRequest {
	oFilters := []ownerName{}
	if config.ownerFilter != "" {
		oFilters = append(oFilters, config.ownerFilter)
	}

	return auctionsRequest{
		RegionName:    config.realm.region.Name,
		RealmSlug:     config.realm.Slug,
		Count:         config.count,
		Page:          config.page,
		SortDirection: config.sortDirection,
		SortKind:      config.sortKind,
		OwnerFilters:  oFilters,
	}
}

func newMiniAuctionListFromBlizzardAuctions(aucs []blizzard.Auction) miniAuctionList {
	// gathering a map of all mini-auctions
	mAuctions := miniAuctions{}
	for _, auc := range aucs {
		maHash := newMiniAuctionHash(auc)
		if mAuction, ok := mAuctions[maHash]; ok {
			mAuction.AucList = append(mAuction.AucList, auc.Auc)
			mAuctions[maHash] = mAuction

			continue
		}

		mAuction := newMiniAuction(auc)
		mAuction.AucList = append(mAuction.AucList, auc.Auc)
		mAuctions[maHash] = mAuction
	}

	maList := miniAuctionList{}
	for _, mAuction := range mAuctions {
		maList = append(maList, mAuction)
	}

	return maList
}

func newMiniAuctionsListFromMessenger(config newMiniAuctionsListFromMessengerConfig) (miniAuctionList, error) {
	am := config.toAuctionsRequest()
	encodedMessage, err := json.Marshal(am)
	if err != nil {
		return miniAuctionList{}, err
	}

	msg, err := config.messenger.request(subjects.Auctions, encodedMessage)
	if err != nil {
		return miniAuctionList{}, err
	}

	if msg.Code != codes.Ok {
		return miniAuctionList{}, errors.New(msg.Err)
	}

	ar, err := newAuctionsResponseFromEncoded([]byte(msg.Data))
	if err != nil {
		return miniAuctionList{}, err
	}

	return ar.AuctionList, nil
}

func newMiniAuctionsList(body []byte) (miniAuctionList, error) {
	maList := &miniAuctionList{}
	if err := json.Unmarshal(body, maList); err != nil {
		return nil, err
	}

	return *maList, nil
}

func newMiniAuctionsListFromGzipped(body []byte) (miniAuctionList, error) {
	gzipDecodedData, err := util.GzipDecode(body)
	if err != nil {
		return miniAuctionList{}, err
	}

	return newMiniAuctionsList(gzipDecodedData)
}

type miniAuctionList []miniAuction

func (maList miniAuctionList) limit(count int, page int) (miniAuctionList, error) {
	alLength := len(maList)
	if alLength == 0 {
		return maList, nil
	}

	start := page * count
	if start > alLength {
		return miniAuctionList{}, fmt.Errorf("Start out of range: %d", start)
	}

	end := start + count
	if end > alLength {
		return maList[start:], nil
	}

	return maList[start:end], nil
}

func (maList miniAuctionList) sort(kind sortkinds.SortKind, direction sortdirections.SortDirection) error {
	mas := newMiniAuctionSorter()
	return mas.sort(kind, direction, maList)
}

func (maList miniAuctionList) filterByOwnerNames(ownerNameFilters []ownerName) miniAuctionList {
	out := miniAuctionList{}
	for _, ma := range maList {
		for _, ownerNameFilter := range ownerNameFilters {
			if ma.Owner == ownerNameFilter {
				out = append(out, ma)
			}
		}
	}

	return out
}

func (maList miniAuctionList) filterByItemIDs(itemIDFilters []blizzard.ItemID) miniAuctionList {
	out := miniAuctionList{}
	for _, ma := range maList {
		for _, itemIDFilter := range itemIDFilters {
			if ma.ItemID == itemIDFilter {
				out = append(out, ma)
			}
		}
	}

	return out
}

func (maList miniAuctionList) itemIds() []blizzard.ItemID {
	result := map[blizzard.ItemID]struct{}{}
	for _, ma := range maList {
		result[ma.ItemID] = struct{}{}
	}

	out := []blizzard.ItemID{}
	for v := range result {
		out = append(out, v)
	}

	return out
}

func (maList miniAuctionList) encodeForDatabase() ([]byte, error) {
	jsonEncodedData, err := json.Marshal(maList)
	if err != nil {
		return []byte{}, err
	}

	gzipEncodedData, err := util.GzipEncode(jsonEncodedData)
	if err != nil {
		return []byte{}, err
	}

	return gzipEncodedData, nil
}

type miniAuctions map[miniAuctionHash]miniAuction

func newMiniAuctionHash(auc blizzard.Auction) miniAuctionHash {
	return miniAuctionHash(fmt.Sprintf(
		"%d-%s-%s-%d-%d-%d-%s",
		auc.Item,
		auc.Owner,
		auc.OwnerRealm,
		auc.Bid,
		auc.Buyout,
		auc.Quantity,
		auc.TimeLeft,
	))
}

type miniAuctionHash string

func newMiniAuction(auc blizzard.Auction) miniAuction {
	var buyoutPer float32
	if auc.Buyout > 0 {
		buyoutPer = float32(auc.Buyout) / float32(auc.Quantity)
	}

	return miniAuction{
		auc.Item,
		ownerName(auc.Owner),
		auc.OwnerRealm,
		auc.Bid,
		auc.Buyout,
		buyoutPer,
		auc.Quantity,
		auc.TimeLeft,
		[]int64{},
	}
}

type miniAuction struct {
	ItemID     blizzard.ItemID `json:"itemId"`
	Owner      ownerName       `json:"owner"`
	OwnerRealm string          `json:"ownerRealm"`
	Bid        int64           `json:"bid"`
	Buyout     int64           `json:"buyout"`
	BuyoutPer  float32         `json:"buyoutPer"`
	Quantity   int64           `json:"quantity"`
	TimeLeft   string          `json:"timeLeft"`
	AucList    []int64         `json:"aucList"`
}
