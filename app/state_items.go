package main

import (
	"encoding/base64"
	"encoding/json"

	"github.com/ihsw/sotah-server/app/blizzard"

	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
	"github.com/ihsw/sotah-server/app/util"
	nats "github.com/nats-io/go-nats"
)

func newItemsRequest(payload []byte) (itemsRequest, error) {
	iRequest := &itemsRequest{}
	err := json.Unmarshal(payload, &iRequest)
	if err != nil {
		return itemsRequest{}, err
	}

	return *iRequest, nil
}

type itemsRequest struct {
	ItemIds []blizzard.ItemID `json:"itemIds"`
}

func (iRequest itemsRequest) resolve(sta state) itemsMap {
	iMap := itemsMap{}

	for _, ID := range iRequest.ItemIds {
		itemValue, ok := sta.items[ID]
		if !ok {
			continue
		}

		iMap[ID] = itemValue
	}

	return iMap
}

type itemsResponse struct {
	Items itemsMap `json:"items"`
}

func (iResponse itemsResponse) encodeForMessage() (string, error) {
	encodedResult, err := json.Marshal(iResponse)
	if err != nil {
		return "", err
	}

	gzippedResult, err := util.GzipEncode(encodedResult)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(gzippedResult), nil
}

func (sta state) listenForItems(stop listenStopChan) error {
	err := sta.messenger.subscribe(subjects.Items, stop, func(natsMsg nats.Msg) {
		m := newMessage()

		// resolving the request
		iRequest, err := newItemsRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		iResponse := itemsResponse{iRequest.resolve(sta)}
		data, err := iResponse.encodeForMessage()
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		m.Data = data
		sta.messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
