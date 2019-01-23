package internal

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/state/sortdirections"
	"github.com/sotah-inc/server/app/pkg/state/sortkinds"
	"github.com/sotah-inc/server/app/pkg/util"
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
	Auctions MiniAuctionList `json:"Auctions"`
}

type newMiniAuctionsListFromMessengerConfig struct {
	realm         Realm
	messenger     messenger.Messenger
	count         int
	page          int
	sortDirection sortdirections.SortDirection
	sortKind      sortkinds.SortKind
	ownerFilter   OwnerName
}

func (config newMiniAuctionsListFromMessengerConfig) toAuctionsRequest() state.AuctionsRequest {
	oFilters := []OwnerName{}
	if config.ownerFilter != "" {
		oFilters = append(oFilters, config.ownerFilter)
	}

	return state.AuctionsRequest{
		RegionName:    config.realm.Region.Name,
		RealmSlug:     config.realm.Slug,
		Count:         config.count,
		Page:          config.page,
		SortDirection: config.sortDirection,
		SortKind:      config.sortKind,
		OwnerFilters:  oFilters,
	}
}

func NewMiniAuctionListFromBlizzardAuctions(aucs []blizzard.Auction) MiniAuctionList {
	// gathering a map of all mini-Auctions
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

	maList := MiniAuctionList{}
	for _, mAuction := range mAuctions {
		maList = append(maList, mAuction)
	}

	return maList
}

func newMiniAuctionsListFromMessenger(config newMiniAuctionsListFromMessengerConfig) (MiniAuctionList, error) {
	am := config.toAuctionsRequest()
	encodedMessage, err := json.Marshal(am)
	if err != nil {
		return MiniAuctionList{}, err
	}

	msg, err := config.messenger.Request(subjects.Auctions, encodedMessage)
	if err != nil {
		return MiniAuctionList{}, err
	}

	if msg.Code != codes.Ok {
		return MiniAuctionList{}, errors.New(msg.Err)
	}

	ar, err := state.NewAuctionsResponseFromEncoded([]byte(msg.Data))
	if err != nil {
		return MiniAuctionList{}, err
	}

	return ar.AuctionList, nil
}

func newMiniAuctionsList(body []byte) (MiniAuctionList, error) {
	maList := &MiniAuctionList{}
	if err := json.Unmarshal(body, maList); err != nil {
		return nil, err
	}

	return *maList, nil
}

func NewMiniAuctionsListFromGzipped(body []byte) (MiniAuctionList, error) {
	gzipDecodedData, err := util.GzipDecode(body)
	if err != nil {
		return MiniAuctionList{}, err
	}

	return newMiniAuctionsList(gzipDecodedData)
}

type MiniAuctionList []miniAuction

func (maList MiniAuctionList) limit(count int, page int) (MiniAuctionList, error) {
	alLength := len(maList)
	if alLength == 0 {
		return maList, nil
	}

	start := page * count
	if start > alLength {
		return MiniAuctionList{}, fmt.Errorf("Start out of range: %d", start)
	}

	end := start + count
	if end > alLength {
		return maList[start:], nil
	}

	return maList[start:end], nil
}

func (maList MiniAuctionList) sort(kind sortkinds.SortKind, direction sortdirections.SortDirection) error {
	mas := newMiniAuctionSorter()
	return mas.sort(kind, direction, maList)
}

func (maList MiniAuctionList) filterByOwnerNames(ownerNameFilters []OwnerName) MiniAuctionList {
	out := MiniAuctionList{}
	for _, ma := range maList {
		for _, ownerNameFilter := range ownerNameFilters {
			if ma.Owner == ownerNameFilter {
				out = append(out, ma)
			}
		}
	}

	return out
}

func (maList MiniAuctionList) filterByItemIDs(itemIDFilters []blizzard.ItemID) MiniAuctionList {
	out := MiniAuctionList{}
	for _, ma := range maList {
		for _, itemIDFilter := range itemIDFilters {
			if ma.ItemID == itemIDFilter {
				out = append(out, ma)
			}
		}
	}

	return out
}

func (maList MiniAuctionList) ItemIds() []blizzard.ItemID {
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

func (maList MiniAuctionList) OwnerNames() []OwnerName {
	result := map[OwnerName]struct{}{}
	for _, ma := range maList {
		result[ma.Owner] = struct{}{}
	}

	out := []OwnerName{}
	for v := range result {
		out = append(out, v)
	}

	return out
}

func (maList MiniAuctionList) TotalAuctions() int {
	out := 0
	for _, auc := range maList {
		out += len(auc.AucList)
	}

	return out
}

func (maList MiniAuctionList) totalQuantity() int {
	out := 0
	for _, auc := range maList {
		out += int(auc.Quantity) * len(auc.AucList)
	}

	return out
}

func (maList MiniAuctionList) totalBuyout() int64 {
	out := int64(0)
	for _, auc := range maList {
		out += auc.Buyout * auc.Quantity * int64(len(auc.AucList))
	}

	return out
}

func (maList MiniAuctionList) AuctionIds() []int64 {
	result := map[int64]struct{}{}
	for _, mAuction := range maList {
		for _, auc := range mAuction.AucList {
			result[auc] = struct{}{}
		}
	}

	out := []int64{}
	for ID := range result {
		out = append(out, ID)
	}

	return out
}

func (maList MiniAuctionList) EncodeForDatabase() ([]byte, error) {
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
		OwnerName(auc.Owner),
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
	Owner      OwnerName       `json:"owner"`
	OwnerRealm string          `json:"ownerRealm"`
	Bid        int64           `json:"bid"`
	Buyout     int64           `json:"buyout"`
	BuyoutPer  float32         `json:"buyoutPer"`
	Quantity   int64           `json:"quantity"`
	TimeLeft   string          `json:"timeLeft"`
	AucList    []int64         `json:"aucList"`
}
