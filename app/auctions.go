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
	log "github.com/sirupsen/logrus"
)

func defaultGetAuctionsURL(url string) string {
	return url
}

type getAuctionsURLFunc func(url string) string

func newAuctionsFromHTTP(url string, r resolver) (auctions, error) {
	body, err := r.get(r.getAuctionsURL(url))
	if err != nil {
		return auctions{}, err
	}

	return newAuctions(body)
}

func newAuctionsFromFilepath(relativeFilepath string) (auctions, error) {
	log.WithField("filepath", relativeFilepath).Info("Reading auctions from file")

	body, err := util.ReadFile(relativeFilepath)
	if err != nil {
		return auctions{}, err
	}

	return newAuctions(body)
}

func newAuctionsFromGzFilepath(rea realm, relativeFilepath string) (auctions, error) {
	body, err := util.ReadFile(relativeFilepath)
	if err != nil {
		return auctions{}, err
	}

	decodedBody, err := util.GzipDecode(body)
	if err != nil {
		return auctions{}, err
	}

	return newAuctions(decodedBody)
}

func newAuctions(body []byte) (auctions, error) {
	a := &auctions{}
	if err := json.Unmarshal(body, a); err != nil {
		return auctions{}, err
	}

	return *a, nil
}

type auctionList []auction

func (al auctionList) minimize() miniAuctionList {
	// gathering a map of all mini-auctions
	mAuctions := miniAuctions{}
	for _, a := range al {
		maHash := a.toMiniAuctionHash()
		if mAuction, ok := mAuctions[maHash]; ok {
			mAuction.AucList = append(mAuction.AucList, a.Auc)
			mAuctions[maHash] = mAuction

			continue
		}

		mAuction := a.toMiniAuction()
		mAuction.AucList = append(mAuction.AucList, a.Auc)
		mAuctions[maHash] = mAuction
	}

	mAuctionList := miniAuctionList{}
	for _, mAuction := range mAuctions {
		mAuctionList = append(mAuctionList, mAuction)
	}

	return mAuctionList
}

type auctions struct {
	Realms   []auctionRealm `json:"realms"`
	Auctions auctionList    `json:"auctions"`
}

type auctionRealm struct {
	Name string    `json:"name"`
	Slug realmSlug `json:"slug"`
}

type auction struct {
	Auc        int64           `json:"auc"`
	Item       blizzard.ItemID `json:"item"`
	Owner      ownerName       `json:"owner"`
	OwnerRealm string          `json:"ownerRealm"`
	Bid        int64           `json:"bid"`
	Buyout     int64           `json:"buyout"`
	Quantity   int64           `json:"quantity"`
	TimeLeft   string          `json:"timeLeft"`
	Rand       int64           `json:"rand"`
	Seed       int64           `json:"seed"`
	Context    int64           `json:"context"`
}

func (auc auction) toMiniAuctionHash() miniAuctionHash {
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

func (auc auction) toMiniAuction() miniAuction {
	var buyoutPer float32
	if auc.Buyout > 0 {
		buyoutPer = float32(auc.Buyout) / float32(auc.Quantity)
	}

	return miniAuction{
		blizzard.Item{ID: auc.Item, Name: "", NormalizedName: ""},
		auc.Owner,
		auc.OwnerRealm,
		auc.Bid,
		auc.Buyout,
		buyoutPer,
		auc.Quantity,
		auc.TimeLeft,
		[]int64{},
	}
}

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

type newMiniAuctionsFromMessengerConfig struct {
	realm         realm
	messenger     messenger
	count         int
	page          int
	sortDirection sortdirections.SortDirection
	sortKind      sortkinds.SortKind
	ownerFilter   ownerName
}

func (config newMiniAuctionsFromMessengerConfig) toAuctionsRequest() auctionsRequest {
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

func newMiniAuctionsFromMessenger(config newMiniAuctionsFromMessengerConfig) (miniAuctionList, error) {
	am := config.toAuctionsRequest()
	encodedMessage, err := json.Marshal(am)
	if err != nil {
		return miniAuctionList{}, err
	}

	log.WithField("subject", subjects.Auctions).Info("Sending request")
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

func newMiniAuctions(body []byte) (miniAuctionList, error) {
	mal := &miniAuctionList{}
	if err := json.Unmarshal(body, mal); err != nil {
		return nil, err
	}

	return *mal, nil
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
			if ma.Item.ID == itemIDFilter {
				out = append(out, ma)
			}
		}
	}

	return out
}

func (maList miniAuctionList) itemIds() []blizzard.ItemID {
	result := map[blizzard.ItemID]struct{}{}
	for _, ma := range maList {
		result[ma.Item.ID] = struct{}{}
	}

	out := []blizzard.ItemID{}
	for v := range result {
		out = append(out, v)
	}

	return out
}

func (maList miniAuctionList) appendItemNames(iMap itemsMap) miniAuctionList {
	for i, mAuction := range maList {
		foundItem, ok := iMap[mAuction.Item.ID]
		if ok {
			maList[i].Item = foundItem
		}
	}

	return maList
}

type miniAuctions map[miniAuctionHash]miniAuction
type miniAuctionHash string

type miniAuction struct {
	Item       blizzard.Item `json:"item"`
	Owner      ownerName     `json:"owner"`
	OwnerRealm string        `json:"ownerRealm"`
	Bid        int64         `json:"bid"`
	Buyout     int64         `json:"buyout"`
	BuyoutPer  float32       `json:"buyoutPer"`
	Quantity   int64         `json:"quantity"`
	TimeLeft   string        `json:"timeLeft"`
	AucList    []int64       `json:"aucList"`
}
