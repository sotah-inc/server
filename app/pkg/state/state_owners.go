package state

import (
	"encoding/json"
	"errors"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"sort"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/internal"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
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
	RegionName internal.RegionName `json:"region_name"`
	RealmSlug  blizzard.RealmSlug  `json:"realm_slug"`
	Query      string              `json:"query"`
}

func (request OwnersRequest) resolve(sta State) (internal.MiniAuctionList, error) {
	regionLadBases, ok := sta.LiveAuctionsDatabases[request.RegionName]
	if !ok {
		return internal.MiniAuctionList{}, errors.New("Invalid region name")
	}

	ladBase, ok := regionLadBases[request.RealmSlug]
	if !ok {
		return internal.MiniAuctionList{}, errors.New("Invalid Realm slug")
	}

	maList, err := ladBase.GetMiniauctions()
	if err != nil {
		return internal.MiniAuctionList{}, err
	}

	return maList, nil
}

func (sta State) ListenForOwners(stop messenger.ListenStopChan) error {
	err := sta.IO.messenger.Subscribe(subjects.Owners, stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		// resolving the request
		request, err := newOwnersRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.IO.messenger.ReplyTo(natsMsg, m)

			return
		}

		// resolving miniauctionslist from the request and State
		mal, err := request.resolve(sta)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.NotFound
			sta.IO.messenger.ReplyTo(natsMsg, m)

			return
		}

		o, err := internal.NewOwnersFromAuctions(mal)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.IO.messenger.ReplyTo(natsMsg, m)

			return
		}

		// optionally filtering in matches
		if request.Query != "" {
			o.Owners = o.Owners.Filter(request.Query)
		}

		// sorting and truncating
		sort.Sort(internal.OwnersByName(o.Owners))
		o.Owners = o.Owners.Limit()

		// marshalling for Messenger
		encodedMessage, err := json.Marshal(o)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.IO.messenger.ReplyTo(natsMsg, m)

			return
		}

		// dumping it out
		m.Data = string(encodedMessage)
		sta.IO.messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}

func (sta State) NewOwners(request OwnersRequest) (sotah.Owners, error) {
	encodedMessage, err := json.Marshal(request)
	if err != nil {
		return sotah.Owners{}, err
	}

	msg, err := sta.IO.messenger.Request(subjects.Owners, encodedMessage)
	if err != nil {
		return sotah.Owners{}, err
	}

	if msg.Code != codes.Ok {
		return sotah.Owners{}, errors.New(msg.Err)
	}

	return sotah.NewOwners([]byte(msg.Data))
}
