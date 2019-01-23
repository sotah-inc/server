package state

import (
	"encoding/base64"
	"encoding/json"

	"github.com/sotah-inc/server/app/pkg/messenger"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/internal"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
	"github.com/sotah-inc/server/app/pkg/util"
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
	ItemIds []blizzard.ItemID `json:"ItemIds"`
}

func (iRequest itemsRequest) resolve(sta State) (internal.ItemsMap, error) {
	return sta.ItemsDatabase.FindItems(iRequest.ItemIds)
}

type itemsResponse struct {
	Items internal.ItemsMap `json:"items"`
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

func (sta State) ListenForItems(stop ListenStopChan) error {
	err := sta.Messenger.Subscribe(subjects.Items, stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		// resolving the request
		iRequest, err := newItemsRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.Messenger.ReplyTo(natsMsg, m)

			return
		}

		iMap, err := iRequest.resolve(sta)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.Messenger.ReplyTo(natsMsg, m)

			return
		}

		iResponse := itemsResponse{iMap}
		data, err := iResponse.encodeForMessage()
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.Messenger.ReplyTo(natsMsg, m)

			return
		}

		m.Data = data
		sta.Messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
