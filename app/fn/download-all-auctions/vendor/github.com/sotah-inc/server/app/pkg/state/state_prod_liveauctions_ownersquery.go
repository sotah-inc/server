package state

import (
	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/database"
	dCodes "github.com/sotah-inc/server/app/pkg/database/codes"
	"github.com/sotah-inc/server/app/pkg/messenger"
	mCodes "github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func (liveAuctionsState ProdLiveAuctionsState) ListenForOwnersQuery(stop ListenStopChan) error {
	err := liveAuctionsState.IO.Messenger.Subscribe(string(subjects.OwnersQuery), stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		// resolving the request
		request, err := database.NewQueryOwnersRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = mCodes.MsgJSONParseError
			liveAuctionsState.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		// querying the live-auctions-databases
		resp, respCode, err := liveAuctionsState.IO.Databases.LiveAuctionsDatabases.QueryOwners(request)
		if respCode != dCodes.Ok {
			m.Err = err.Error()
			m.Code = DatabaseCodeToMessengerCode(respCode)
			liveAuctionsState.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		// marshalling for messenger
		encodedMessage, err := resp.EncodeForDelivery()
		if err != nil {
			m.Err = err.Error()
			m.Code = mCodes.GenericError
			liveAuctionsState.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		// dumping it out
		m.Data = string(encodedMessage)
		liveAuctionsState.IO.Messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
