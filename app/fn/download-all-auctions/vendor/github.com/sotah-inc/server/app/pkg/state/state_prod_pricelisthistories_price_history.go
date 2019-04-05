package state

import (
	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/database"
	dCodes "github.com/sotah-inc/server/app/pkg/database/codes"
	"github.com/sotah-inc/server/app/pkg/messenger"
	mCodes "github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func (phState ProdPricelistHistoriesState) ListenForPriceListHistory(stop ListenStopChan) error {
	err := phState.IO.Messenger.Subscribe(string(subjects.PriceListHistory), stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		// resolving the request
		request, err := database.NewGetPricelistHistoryRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = mCodes.MsgJSONParseError
			phState.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		// querying the live-auctions-databases
		resp, respCode, err := phState.IO.Databases.PricelistHistoryDatabases.GetPricelistHistory(request)
		if respCode != dCodes.Ok {
			m.Err = err.Error()
			m.Code = DatabaseCodeToMessengerCode(respCode)
			phState.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		// marshalling for messenger
		encodedMessage, err := resp.EncodeForDelivery()
		if err != nil {
			m.Err = err.Error()
			m.Code = mCodes.GenericError
			phState.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		// dumping it out
		m.Data = string(encodedMessage)
		phState.IO.Messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
