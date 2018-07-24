package main

import (
	"encoding/json"

	"github.com/ihsw/sotah-server/app/blizzard"

	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
	nats "github.com/nats-io/go-nats"
)

type itemsResult struct {
	Items itemsMap
}

func newItemsRequest(payload []byte) (itemsRequest, error) {
	iRequest := &itemsRequest{}
	err := json.Unmarshal(payload, &iRequest)
	if err != nil {
		return itemsRequest{}, err
	}

	return *iRequest, nil
}

type itemsRequest struct {
	itemIds []blizzard.ItemID
}

func (iRequest itemsRequest) resolve(sta state) itemsResult {
	iResult := itemsResult{Items: itemsMap{}}

	for _, ID := range iRequest.itemIds {
		itemValue, ok := sta.items[ID]
		if !ok {
			continue
		}

		iResult.Items[ID] = itemValue
	}

	return iResult
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

		iResult := iRequest.resolve(sta)

		encodedResult, err := json.Marshal(iResult)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedResult)
		sta.messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
