package state

import (
	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/messenger"
	mCodes "github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func (sta ProdApiState) ListenForQueryRealmModificationDates(stop ListenStopChan) error {
	err := sta.IO.Messenger.Subscribe(string(subjects.QueryRealmModificationDates), stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		req, err := NewRealmModificationDatesRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = mCodes.GenericError
			sta.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		logging.WithField("hell-region-realms", sta.HellRegionRealms.Total()).Info("Checking hell-region-realms")

		hellRealms, ok := sta.HellRegionRealms[blizzard.RegionName(req.RegionName)]
		if !ok {
			m.Err = "region not found"
			m.Code = mCodes.NotFound
			sta.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		hellRealm, ok := hellRealms[blizzard.RealmSlug(req.RealmSlug)]
		if !ok {
			m.Err = "realm not found"
			m.Code = mCodes.NotFound
			sta.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		res := RealmModificationDatesResponse{
			RealmModificationDates: sotah.RealmModificationDates{
				Downloaded:                 int64(hellRealm.Downloaded),
				LiveAuctionsReceived:       int64(hellRealm.LiveAuctionsReceived),
				PricelistHistoriesReceived: int64(hellRealm.PricelistHistoriesReceived),
			},
		}

		encodedData, err := res.EncodeForDelivery()
		if err != nil {
			m.Err = err.Error()
			m.Code = mCodes.GenericError
			sta.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedData)
		sta.IO.Messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
