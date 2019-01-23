package state

import (
	"encoding/base64"
	"encoding/json"

	"github.com/sotah-inc/server/app/blizzard"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/codes"
	"github.com/sotah-inc/server/app/subjects"
	"github.com/sotah-inc/server/app/util"
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

func (iRequest itemsRequest) resolve(sta State) (itemsMap, error) {
	return sta.itemsDatabase.findItems(iRequest.ItemIds)
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

func (sta State) listenForItems(stop listenStopChan) error {
	err := sta.Messenger.subscribe(subjects.Items, stop, func(natsMsg nats.Msg) {
		m := newMessage()

		// resolving the request
		iRequest, err := newItemsRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.Messenger.replyTo(natsMsg, m)

			return
		}

		iMap, err := iRequest.resolve(sta)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.Messenger.replyTo(natsMsg, m)

			return
		}

		iResponse := itemsResponse{iMap}
		data, err := iResponse.encodeForMessage()
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.Messenger.replyTo(natsMsg, m)

			return
		}

		m.Data = data
		sta.Messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
