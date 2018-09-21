package main

import (
	"encoding/json"
	"errors"
	"sort"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
	nats "github.com/nats-io/go-nats"
)

func newOwnersRequest(payload []byte) (ownersRequest, error) {
	request := &ownersRequest{}
	err := json.Unmarshal(payload, &request)
	if err != nil {
		return ownersRequest{}, err
	}

	return *request, nil
}

type ownersRequest struct {
	RegionName regionName         `json:"region_name"`
	RealmSlug  blizzard.RealmSlug `json:"realm_slug"`
	Query      string             `json:"query"`
}

func (request ownersRequest) resolve(sta state) (miniAuctionList, error) {
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

func (sta state) listenForOwners(stop listenStopChan) error {
	err := sta.messenger.subscribe(subjects.Owners, stop, func(natsMsg nats.Msg) {
		m := newMessage()

		// resolving the request
		request, err := newOwnersRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		// resolving miniauctionslist from the request and state
		mal, err := request.resolve(sta)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.NotFound
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		o, err := newOwnersFromAuctions(mal)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		// optionally filtering in matches
		if request.Query != "" {
			o.Owners = o.Owners.filter(request.Query)
		}

		// sorting and truncating
		sort.Sort(ownersByName(o.Owners))
		o.Owners = o.Owners.limit()

		// marshalling for messenger
		encodedMessage, err := json.Marshal(o)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		// dumping it out
		m.Data = string(encodedMessage)
		sta.messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
