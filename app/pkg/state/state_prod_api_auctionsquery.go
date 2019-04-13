package state

import (
	"encoding/json"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/messenger"
	mCodes "github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func NewAuctionsQueryRequest(data []byte) (AuctionsQueryRequest, error) {
	var out AuctionsQueryRequest
	if err := json.Unmarshal(data, &out); err != nil {
		return AuctionsQueryRequest{}, err
	}

	return out, nil
}

type AuctionsQueryRequest struct {
	Query      string `json:"query"`
	RegionName string `json:"region_name"`
	RealmSlug  string `json:"realm_slug"`
}

func (sta ProdApiState) ListenForAuctionsQuery(stop ListenStopChan) error {
	err := sta.IO.Messenger.Subscribe(string(subjects.AuctionsQuery), stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		auctionsQueryRequest, err := NewAuctionsQueryRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = mCodes.GenericError
			sta.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		//queryItemsRequest := database.QueryItemsRequest{Query: auctionsQueryRequest.Query}
		//
		//queryOwnersRequest := database.QueryOwnersRequest{
		//	Query:      auctionsQueryRequest.Query,
		//	RegionName: blizzard.RegionName(auctionsQueryRequest.RegionName),
		//	RealmSlug:  blizzard.RealmSlug(auctionsQueryRequest.RealmSlug),
		//}

		encodedData, err := json.Marshal(auctionsQueryRequest)
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
