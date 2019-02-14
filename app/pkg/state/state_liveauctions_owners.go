package state

import (
	"encoding/json"
	"errors"
	"sort"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

func newOwnersRequest(payload []byte) (OwnersRequest, error) {
	request := &OwnersRequest{}
	err := json.Unmarshal(payload, &request)
	if err != nil {
		return OwnersRequest{}, err
	}

	return *request, nil
}

type OwnersRequest struct {
	RegionName blizzard.RegionName `json:"region_name"`
	RealmSlug  blizzard.RealmSlug  `json:"realm_slug"`
	Query      string              `json:"query"`
}

func (request OwnersRequest) resolve(sta LiveAuctionsState) (sotah.MiniAuctionList, error) {
	regionLadBases, ok := sta.IO.Databases.LiveAuctionsDatabases[request.RegionName]
	if !ok {
		return sotah.MiniAuctionList{}, errors.New("invalid region name")
	}

	ladBase, ok := regionLadBases[request.RealmSlug]
	if !ok {
		return sotah.MiniAuctionList{}, errors.New("invalid realm slug")
	}

	maList, err := ladBase.GetMiniAuctionList()
	if err != nil {
		return sotah.MiniAuctionList{}, err
	}

	return maList, nil
}

func (sta LiveAuctionsState) ListenForOwners(stop ListenStopChan) error {
	err := sta.IO.Messenger.Subscribe(string(subjects.Owners), stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		// resolving the request
		request, err := newOwnersRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		// resolving mini-auctions-list from the request and State
		mal, err := request.resolve(sta)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.NotFound
			sta.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		o, err := sotah.NewOwnersFromAuctions(mal)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		// optionally filtering in matches
		if request.Query != "" {
			o.Owners = o.Owners.Filter(request.Query)
		}

		// sorting and truncating
		sort.Sort(sotah.OwnersByName(o.Owners))
		o.Owners = o.Owners.Limit()

		// marshalling for Messenger
		encodedMessage, err := json.Marshal(o)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		// dumping it out
		m.Data = string(encodedMessage)
		sta.IO.Messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
