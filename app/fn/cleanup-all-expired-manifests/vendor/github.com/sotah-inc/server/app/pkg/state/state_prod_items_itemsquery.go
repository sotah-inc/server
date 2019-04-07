package state

import (
	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/database"
	dCodes "github.com/sotah-inc/server/app/pkg/database/codes"
	"github.com/sotah-inc/server/app/pkg/messenger"
	mCodes "github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func (itemsState ProdItemsState) ListenForItemsQuery(stop ListenStopChan) error {
	err := itemsState.IO.Messenger.Subscribe(string(subjects.ItemsQuery), stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		// resolving the request
		request, err := database.NewQueryItemsRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = mCodes.MsgJSONParseError
			itemsState.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		// querying the items-database
		resp, respCode, err := itemsState.IO.Databases.ItemsDatabase.QueryItems(request)
		if respCode != dCodes.Ok {
			m.Err = err.Error()
			m.Code = DatabaseCodeToMessengerCode(respCode)
			itemsState.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		// marshalling for messenger
		encodedMessage, err := resp.EncodeForDelivery()
		if err != nil {
			m.Err = err.Error()
			m.Code = mCodes.GenericError
			itemsState.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		// dumping it out
		m.Data = string(encodedMessage)
		itemsState.IO.Messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
