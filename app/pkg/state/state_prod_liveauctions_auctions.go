package state

import (
	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/database"
	dCodes "github.com/sotah-inc/server/app/pkg/database/codes"
	"github.com/sotah-inc/server/app/pkg/messenger"
	mCodes "github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func (liveAuctionsState ProdLiveAuctionsState) ListenForAuctions(stop ListenStopChan) error {
	err := liveAuctionsState.IO.Messenger.Subscribe(string(subjects.Auctions), stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		// resolving the request
		qRequest, err := database.NewQueryRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = mCodes.MsgJSONParseError
			liveAuctionsState.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		qResponse, dCode, err := liveAuctionsState.IO.Databases.LiveAuctionsDatabases.Query(qRequest)
		if dCode != dCodes.Ok {
			m.Err = err.Error()
			m.Code = DatabaseCodeToMessengerCode(dCode)
			liveAuctionsState.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		// encoding the auctions list for output
		data, err := qResponse.EncodeForDelivery()
		if err != nil {
			m.Err = err.Error()
			m.Code = mCodes.GenericError
			liveAuctionsState.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		m.Data = data
		liveAuctionsState.IO.Messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
