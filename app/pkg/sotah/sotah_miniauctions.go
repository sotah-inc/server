package sotah

import (
	"encoding/json"
	"fmt"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/sotah/sortdirections"
	"github.com/sotah-inc/server/app/pkg/sotah/sortkinds"
	"github.com/sotah-inc/server/app/pkg/util"
)

// miniauction
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

// miniauction-list
func NewMiniAuctionListFromMiniAuctions(ma MiniAuctions) MiniAuctionList {
	out := MiniAuctionList{}
	for _, mAuction := range ma {
		out = append(out, mAuction)
	}

	return out
}

func NewMiniAuctionListFromGzipped(body []byte) (MiniAuctionList, error) {
	gzipDecodedData, err := util.GzipDecode(body)
	if err != nil {
		return MiniAuctionList{}, err
	}

	return newMiniAuctionList(gzipDecodedData)
}

func newMiniAuctionList(body []byte) (MiniAuctionList, error) {
	maList := &MiniAuctionList{}
	if err := json.Unmarshal(body, maList); err != nil {
		return nil, err
	}

	return *maList, nil
}

type MiniAuctionList []miniAuction

func (maList MiniAuctionList) Limit(count int, page int) (MiniAuctionList, error) {
	alLength := len(maList)
	if alLength == 0 {
		return maList, nil
	}

	start := page * count
	if start > alLength {
		return MiniAuctionList{}, fmt.Errorf("start out of range: %d", start)
	}

	end := start + count
	if end > alLength {
		return maList[start:], nil
	}

	return maList[start:end], nil
}

func (maList MiniAuctionList) Sort(kind sortkinds.SortKind, direction sortdirections.SortDirection) error {
	mas := newMiniAuctionSorter()
	return mas.sort(kind, direction, maList)
}

func (maList MiniAuctionList) FilterByOwnerNames(ownerNameFilters []OwnerName) MiniAuctionList {
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

func (maList MiniAuctionList) FilterByItemIDs(itemIDFilters []blizzard.ItemID) MiniAuctionList {
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

func (maList MiniAuctionList) TotalQuantity() int {
	out := 0
	for _, auc := range maList {
		out += int(auc.Quantity) * len(auc.AucList)
	}

	return out
}

func (maList MiniAuctionList) TotalBuyout() int64 {
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

// mini-auctions
func NewMiniAuctions(aucs blizzard.Auctions) MiniAuctions {
	out := MiniAuctions{}
	for _, auc := range aucs.Auctions {
		maHash := newMiniAuctionHash(auc)
		if mAuction, ok := out[maHash]; ok {
			mAuction.AucList = append(mAuction.AucList, auc.Auc)
			out[maHash] = mAuction

			continue
		}

		mAuction := newMiniAuction(auc)
		mAuction.AucList = append(mAuction.AucList, auc.Auc)
		out[maHash] = mAuction
	}

	return out
}

type MiniAuctions map[miniAuctionHash]miniAuction

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
