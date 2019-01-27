package state

import (
	"encoding/base64"
	"encoding/json"
	"errors"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/internal"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
	"github.com/sotah-inc/server/app/pkg/util"
)

func newPriceListRequest(payload []byte) (priceListRequest, error) {
	pList := &priceListRequest{}
	err := json.Unmarshal(payload, &pList)
	if err != nil {
		return priceListRequest{}, err
	}

	return *pList, nil
}

type priceListRequest struct {
	RegionName internal.RegionName `json:"region_name"`
	RealmSlug  blizzard.RealmSlug  `json:"realm_slug"`
	ItemIds    []blizzard.ItemID   `json:"item_ids"`
}

func (plRequest priceListRequest) resolve(sta State) (internal.MiniAuctionList, requestError) {
	regionLadBases, ok := sta.LiveAuctionsDatabases[plRequest.RegionName]
	if !ok {
		return internal.MiniAuctionList{}, requestError{codes.NotFound, "Invalid region"}
	}

	ladBase, ok := regionLadBases[plRequest.RealmSlug]
	if !ok {
		return internal.MiniAuctionList{}, requestError{codes.NotFound, "Invalid Realm"}
	}

	maList, err := ladBase.GetMiniauctions()
	if err != nil {
		return internal.MiniAuctionList{}, requestError{codes.GenericError, err.Error()}
	}

	return maList, requestError{codes.Ok, ""}
}

func newPriceListResponseFromMessenger(plRequest priceListRequest, mess messenger.Messenger) (priceListResponse, error) {
	encodedMessage, err := json.Marshal(plRequest)
	if err != nil {
		return priceListResponse{}, err
	}

	msg, err := mess.Request(subjects.PriceList, encodedMessage)
	if err != nil {
		return priceListResponse{}, err
	}

	if msg.Code != codes.Ok {
		return priceListResponse{}, errors.New(msg.Err)
	}

	return newPriceListResponse([]byte(msg.Data))
}

func newPriceListResponse(body []byte) (priceListResponse, error) {
	plResponse := &priceListResponse{}
	if err := json.Unmarshal(body, &plResponse); err != nil {
		return priceListResponse{}, err
	}

	return *plResponse, nil
}

type priceListResponse struct {
	PriceList PriceList `json:"price_list"`
}

func (plResponse priceListResponse) encodeForMessage() (string, error) {
	jsonEncodedMessage, err := json.Marshal(plResponse)
	if err != nil {
		return "", err
	}

	gzipEncodedMessage, err := util.GzipEncode(jsonEncodedMessage)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(gzipEncodedMessage), nil
}

func (sta State) ListenForPriceList(stop messenger.ListenStopChan) error {
	err := sta.IO.messenger.Subscribe(subjects.PriceList, stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		// resolving the request
		plRequest, err := newPriceListRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.IO.messenger.ReplyTo(natsMsg, m)

			return
		}

		// resolving data from State
		realmAuctions, reErr := plRequest.resolve(sta)
		if reErr.code != codes.Ok {
			m.Err = reErr.message
			m.Code = reErr.code
			sta.IO.messenger.ReplyTo(natsMsg, m)

			return
		}

		// deriving a pricelist-response from the provided Realm auctions
		plResponse := priceListResponse{NewPriceList(plRequest.ItemIds, realmAuctions)}
		data, err := plResponse.encodeForMessage()
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.IO.messenger.ReplyTo(natsMsg, m)

			return
		}

		m.Data = data
		sta.IO.messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
