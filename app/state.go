package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ihsw/sotah-server/app/codes"

	"github.com/ihsw/sotah-server/app/subjects"
	"github.com/ihsw/sotah-server/app/util"
	nats "github.com/nats-io/go-nats"
)

type state struct {
	messenger messenger
	resolver  *resolver

	regions  []region
	statuses map[regionName]*status
	auctions map[regionName]map[realmSlug]*auctions
}

type statusRequest struct {
	RegionName regionName `json:"region_name"`
}

func (sta state) listenForStatus(stop chan interface{}) error {
	err := sta.messenger.subscribe(subjects.Status, stop, func(natsMsg *nats.Msg) {
		m := newMessage()

		sr := &statusRequest{}
		err := json.Unmarshal(natsMsg.Data, &sr)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		var reg region
		for _, r := range sta.regions {
			if r.Name != sr.RegionName {
				continue
			}

			reg = r
			break
		}

		if reg.Name == "" {
			m.Err = "Invalid region"
			m.Code = codes.NotFound
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		regionStatus, ok := sta.statuses[sr.RegionName]
		if !ok {
			if sta.resolver == nil {
				m.Err = "Resolver not defined"
				m.Code = codes.GenericError
				sta.messenger.replyTo(natsMsg, m)

				return
			}

			regionStatus, err = reg.getStatus(*sta.resolver)
			if err != nil {
				m.Err = fmt.Sprintf("Could not fetch region: %s", err.Error())
				m.Code = codes.GenericError
				sta.messenger.replyTo(natsMsg, m)

				return
			}

			if regionStatus == nil {
				m.Err = "Region-status was nil"
				m.Code = codes.GenericError
				sta.messenger.replyTo(natsMsg, m)

				return
			}

			sta.statuses[reg.Name] = regionStatus
			for _, realm := range regionStatus.Realms {
				sta.auctions[reg.Name][realm.Slug] = &auctions{}
			}
		}

		encodedStatus, err := json.Marshal(regionStatus)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedStatus)
		sta.messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}

func newAuctionsRequest(payload []byte) (*auctionsRequest, error) {
	ar := &auctionsRequest{}
	err := json.Unmarshal(payload, &ar)
	if err != nil {
		return &auctionsRequest{}, err
	}

	return ar, nil
}

type auctionsRequest struct {
	RegionName regionName `json:"region_name"`
	RealmSlug  realmSlug  `json:"realm_slug"`
}

func (l auctionsRequest) validate(sta state) error {
	regionAuctions, ok := sta.auctions[l.RegionName]
	if !ok {
		return errors.New("Invalid region")
	}

	_, ok = regionAuctions[l.RealmSlug]
	if !ok {
		return errors.New("Invalid realm")
	}

	return nil
}

func (sta state) listenForAuctions(stop chan interface{}) error {
	err := sta.messenger.subscribe(subjects.Auctions, stop, func(natsMsg *nats.Msg) {
		m := newMessage()

		ar, err := newAuctionsRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		err = ar.validate(sta)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.NotFound
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		auctions, ok := sta.auctions[ar.RegionName][ar.RealmSlug]
		if !ok {
			m.Err = "Invalid realm"
			m.Code = codes.NotFound
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		jsonEncodedAuctions, err := json.Marshal(auctions)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		gzipEncodedAuctions, err := util.GzipEncode(jsonEncodedAuctions)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		base64EncodedAuctions := base64.StdEncoding.EncodeToString(gzipEncodedAuctions)

		m.Data = base64EncodedAuctions
		sta.messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}

func (sta state) listenForRegions(stop chan interface{}) error {
	err := sta.messenger.subscribe(subjects.Regions, stop, func(natsMsg *nats.Msg) {
		m := newMessage()

		encodedRegions, err := json.Marshal(sta.regions)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		m.Data = string(encodedRegions)
		sta.messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}

func (sta state) listenForGenericTestErrors(stop chan interface{}) error {
	err := sta.messenger.subscribe(subjects.GenericTestErrors, stop, func(natsMsg *nats.Msg) {
		m := newMessage()
		m.Err = "Test error"
		m.Code = codes.GenericError
		sta.messenger.replyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
