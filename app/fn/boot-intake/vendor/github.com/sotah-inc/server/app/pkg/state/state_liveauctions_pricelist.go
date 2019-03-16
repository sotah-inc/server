package state

import (
	"encoding/base64"
	"encoding/json"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
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
	RegionName blizzard.RegionName `json:"region_name"`
	RealmSlug  blizzard.RealmSlug  `json:"realm_slug"`
	ItemIds    []blizzard.ItemID   `json:"item_ids"`
}

func (plRequest priceListRequest) resolve(laState LiveAuctionsState) (sotah.MiniAuctionList, requestError) {
	regionLadBases, ok := laState.IO.Databases.LiveAuctionsDatabases[plRequest.RegionName]
	if !ok {
		return sotah.MiniAuctionList{}, requestError{codes.NotFound, "Invalid region"}
	}

	ladBase, ok := regionLadBases[plRequest.RealmSlug]
	if !ok {
		return sotah.MiniAuctionList{}, requestError{codes.NotFound, "Invalid realm"}
	}

	maList, err := ladBase.GetMiniAuctionList()
	if err != nil {
		return sotah.MiniAuctionList{}, requestError{codes.GenericError, err.Error()}
	}

	return maList, requestError{codes.Ok, ""}
}

type priceListResponse struct {
	PriceList sotah.ItemPrices `json:"price_list"`
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

func (laState LiveAuctionsState) ListenForPriceList(stop ListenStopChan) error {
	err := laState.IO.Messenger.Subscribe(string(subjects.PriceList), stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		// resolving the request
		plRequest, err := newPriceListRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			laState.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		// resolving data from state
		realmAuctions, reErr := plRequest.resolve(laState)
		if reErr.code != codes.Ok {
			m.Err = reErr.message
			m.Code = reErr.code
			laState.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		// deriving a pricelist-response from the provided realm auctions
		iPrices := sotah.NewItemPrices(realmAuctions)
		responseItemPrices := sotah.ItemPrices{}
		for _, itemId := range plRequest.ItemIds {
			if iPrice, ok := iPrices[itemId]; ok {
				responseItemPrices[itemId] = iPrice

				continue
			}
		}

		plResponse := priceListResponse{responseItemPrices}
		data, err := plResponse.encodeForMessage()
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			laState.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		m.Data = data
		laState.IO.Messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
