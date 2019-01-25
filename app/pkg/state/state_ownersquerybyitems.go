package state

import (
	"encoding/json"

	"github.com/sotah-inc/server/app/internal"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
)

type ownerItemsOwnership struct {
	OwnedValue  int64 `json:"owned_value"`
	OwnedVolume int64 `json:"owned_volume"`
}

type ownersQueryResultByItems struct {
	Ownership   map[internal.OwnerName]ownerItemsOwnership `json:"ownership"`
	TotalValue  int64                                      `json:"total_value"`
	TotalVolume int64                                      `json:"total_volume"`
}

func newOwnersQueryRequestByItem(payload []byte) (ownersQueryRequestByItems, error) {
	request := &ownersQueryRequestByItems{}
	err := json.Unmarshal(payload, &request)
	if err != nil {
		return ownersQueryRequestByItems{}, err
	}

	return *request, nil
}

type ownersQueryRequestByItems struct {
	RegionName internal.RegionName `json:"region_name"`
	RealmSlug  blizzard.RealmSlug  `json:"realm_slug"`
	Items      []blizzard.ItemID   `json:"items"`
}

func (request ownersQueryRequestByItems) resolve(sta State) (internal.MiniAuctionList, requestError) {
	regionLadBases, ok := sta.LiveAuctionsDatabases[request.RegionName]
	if !ok {
		return internal.MiniAuctionList{}, requestError{codes.NotFound, "Invalid region"}
	}

	ladBase, ok := regionLadBases[request.RealmSlug]
	if !ok {
		return internal.MiniAuctionList{}, requestError{codes.NotFound, "Invalid Realm"}
	}

	maList, err := ladBase.GetMiniauctions()
	if err != nil {
		return internal.MiniAuctionList{}, requestError{codes.GenericError, err.Error()}
	}

	return maList, requestError{codes.Ok, ""}
}

func (sta State) ListenForOwnersQueryByItems(stop messenger.ListenStopChan) error {
	err := sta.IO.messenger.Subscribe(subjects.OwnersQueryByItems, stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		// resolving the request
		request, err := newOwnersQueryRequestByItem(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.IO.messenger.ReplyTo(natsMsg, m)

			return
		}

		iMap := map[blizzard.ItemID]struct{}{}
		for _, ID := range request.Items {
			iMap[ID] = struct{}{}
		}

		// resolving the auctions
		aList, reErr := request.resolve(sta)
		if reErr.code != codes.Ok {
			m.Err = reErr.message
			m.Code = reErr.code
			sta.IO.messenger.ReplyTo(natsMsg, m)

			return
		}

		// going over the auctions to gather results
		result := ownersQueryResultByItems{
			Ownership:   map[internal.OwnerName]ownerItemsOwnership{},
			TotalValue:  0,
			TotalVolume: 0,
		}
		for _, mAuction := range aList {
			if _, ok := iMap[mAuction.ItemID]; !ok {
				continue
			}

			aucListValue := mAuction.Buyout * mAuction.Quantity * int64(len(mAuction.AucList))
			aucListVolume := int64(len(mAuction.AucList)) * mAuction.Quantity

			result.TotalValue += aucListValue
			result.TotalVolume += aucListVolume

			if _, ok := result.Ownership[mAuction.Owner]; !ok {
				result.Ownership[mAuction.Owner] = ownerItemsOwnership{0, 0}
			}

			result.Ownership[mAuction.Owner] = ownerItemsOwnership{
				OwnedValue:  result.Ownership[mAuction.Owner].OwnedValue + aucListValue,
				OwnedVolume: result.Ownership[mAuction.Owner].OwnedVolume + aucListVolume,
			}
		}

		// marshalling for Messenger
		encodedMessage, err := json.Marshal(result)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.IO.messenger.ReplyTo(natsMsg, m)

			return
		}

		// dumping it out
		m.Data = string(encodedMessage)
		sta.IO.messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
