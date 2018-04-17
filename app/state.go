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
	auctions map[regionName]map[realmSlug]miniAuctionList
}

type requestError struct {
	code    int
	message string
}

func newStatusRequest(payload []byte) (*statusRequest, error) {
	sr := &statusRequest{}
	err := json.Unmarshal(payload, &sr)
	if err != nil {
		return nil, err
	}

	return sr, nil
}

type statusRequest struct {
	RegionName regionName `json:"region_name"`
}

func (sr statusRequest) resolve(sta state) (region, error) {
	var reg region
	for _, r := range sta.regions {
		if r.Name != sr.RegionName {
			continue
		}

		reg = r
		break
	}

	if reg.Name == "" {
		return region{}, errors.New("Invalid region")
	}

	return reg, nil
}

func (sta state) listenForStatus(stop chan interface{}) error {
	err := sta.messenger.subscribe(subjects.Status, stop, func(natsMsg *nats.Msg) {
		m := newMessage()

		sr, err := newStatusRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		reg, err := sr.resolve(sta)
		if err != nil {
			m.Err = err.Error()
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
				sta.auctions[reg.Name][realm.Slug] = miniAuctionList{}
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
	Page       int
	Count      int
}

func (ar auctionsRequest) resolve(sta state) (miniAuctionList, requestError) {
	regionAuctions, ok := sta.auctions[ar.RegionName]
	if !ok {
		return miniAuctionList{}, requestError{codes.NotFound, "Invalid region"}
	}

	realmAuctions, ok := regionAuctions[ar.RealmSlug]
	if !ok {
		return miniAuctionList{}, requestError{codes.NotFound, "Invalid realm"}
	}

	if ar.Page < 0 {
		return miniAuctionList{}, requestError{codes.UserError, "Page must be >=0"}
	}
	if ar.Count == 0 {
		return miniAuctionList{}, requestError{codes.UserError, "Count must be >0"}
	} else if ar.Count > 1000 {
		return miniAuctionList{}, requestError{codes.UserError, "Count must be <=1000"}
	}

	return realmAuctions, requestError{codes.Ok, ""}
}

type auctionsResponse struct {
	AuctionList miniAuctionList `json:"auctions"`
	Total       int             `json:"total"`
	TotalCount  int             `json:"total_count"`
}

func (ar auctionsResponse) encodeForMessage() (string, error) {
	jsonEncodedAuctions, err := json.Marshal(ar)
	if err != nil {
		return "", err
	}

	gzipEncodedAuctions, err := util.GzipEncode(jsonEncodedAuctions)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(gzipEncodedAuctions), nil
}

func (sta state) listenForAuctions(stop chan interface{}) error {
	err := sta.messenger.subscribe(subjects.Auctions, stop, func(natsMsg *nats.Msg) {
		m := newMessage()

		aRequest, err := newAuctionsRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		realmAuctions, reErr := aRequest.resolve(sta)
		if reErr.code != codes.Ok {
			m.Err = reErr.message
			m.Code = reErr.code
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		totalCount := 0
		for _, mAuction := range realmAuctions {
			totalCount += len(mAuction.AucList)
		}

		aResponse := auctionsResponse{Total: len(realmAuctions), TotalCount: totalCount}
		aResponse.AuctionList, err = realmAuctions.limit(aRequest.Count, aRequest.Page)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.UserError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		data, err := aResponse.encodeForMessage()
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		m.Data = data
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
