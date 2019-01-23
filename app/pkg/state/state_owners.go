package state

import (
	"encoding/json"
	"errors"
	"sort"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/internal"
	"github.com/sotah-inc/server/app/pkg/blizzard"
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

func (request OwnersRequest) resolve(sta State) (miniAuctionList, error) {
	regionLadBases, ok := sta.liveAuctionsDatabases[request.RegionName]
	if !ok {
		return miniAuctionList{}, errors.New("Invalid region name")
	}

	ladBase, ok := regionLadBases[request.RealmSlug]
	if !ok {
		return miniAuctionList{}, errors.New("Invalid realm slug")
	}

	maList, err := ladBase.getMiniauctions()
	if err != nil {
		return miniAuctionList{}, err
	}

	return maList, nil
}

func (sta State) listenForOwners(stop listenStopChan) error {
	err := sta.Messenger.subscribe(subjects.Owners, stop, func(natsMsg nats.Msg) {
		m := newMessage()

		// resolving the request
		request, err := newOwnersRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.Messenger.replyTo(natsMsg, m)

			return
		}

		// resolving miniauctionslist from the request and State
		mal, err := request.resolve(sta)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.NotFound
			sta.Messenger.replyTo(natsMsg, m)

			return
		}

		o, err := newOwnersFromAuctions(mal)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.Messenger.replyTo(natsMsg, m)

			return
		}

		// optionally filtering in matches
		if request.Query != "" {
			o.Owners = o.Owners.filter(request.Query)
		}

		// sorting and truncating
		sort.Sort(ownersByName(o.Owners))
		o.Owners = o.Owners.limit()

		// marshalling for Messenger
		encodedMessage, err := json.Marshal(o)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.Messenger.replyTo(natsMsg, m)

			return
		}

		// dumping it out
		m.Data = string(encodedMessage)
		sta.Messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
