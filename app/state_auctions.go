package main

import (
	"encoding/base64"
	"encoding/json"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/sortdirections"
	"github.com/ihsw/sotah-server/app/sortkinds"
	"github.com/ihsw/sotah-server/app/subjects"
	"github.com/ihsw/sotah-server/app/util"
	nats "github.com/nats-io/go-nats"
)

func newAuctionsRequest(payload []byte) (auctionsRequest, error) {
	ar := &auctionsRequest{}
	err := json.Unmarshal(payload, &ar)
	if err != nil {
		return auctionsRequest{}, err
	}

	return *ar, nil
}

type auctionsRequest struct {
	RegionName    regionName                   `json:"region_name"`
	RealmSlug     blizzard.RealmSlug           `json:"realm_slug"`
	Page          int                          `json:"page"`
	Count         int                          `json:"count"`
	SortDirection sortdirections.SortDirection `json:"sort_direction"`
	SortKind      sortkinds.SortKind           `json:"sort_kind"`
	OwnerFilters  []ownerName                  `json:"owner_filters"`
	ItemFilters   []blizzard.ItemID            `json:"item_filters"`
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

	result := make(miniAuctionList, len(realmAuctions))
	copy(result, realmAuctions)

	return result, requestError{codes.Ok, ""}
}

func newAuctionsResponseFromEncoded(body []byte) (auctionsResponse, error) {
	base64Decoded, err := base64.StdEncoding.DecodeString(string(body))
	if err != nil {
		return auctionsResponse{}, err
	}

	gzipDecoded, err := util.GzipDecode(base64Decoded)
	if err != nil {
		return auctionsResponse{}, err
	}

	return newAuctionsResponse(gzipDecoded)
}

func newAuctionsResponse(body []byte) (auctionsResponse, error) {
	ar := &auctionsResponse{}
	if err := json.Unmarshal(body, ar); err != nil {
		return auctionsResponse{}, err
	}

	return *ar, nil
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
	err := sta.messenger.subscribe(subjects.Auctions, stop, func(natsMsg nats.Msg) {
		m := newMessage()

		// resolving the request
		aRequest, err := newAuctionsRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		// resolving data from state
		realmAuctions, reErr := aRequest.resolve(sta)
		if reErr.code != codes.Ok {
			m.Err = reErr.message
			m.Code = reErr.code
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		// initial response format
		aResponse := auctionsResponse{Total: -1, TotalCount: -1, AuctionList: realmAuctions}

		// filtering in auctions by owners or items
		if len(aRequest.OwnerFilters) > 0 {
			aResponse.AuctionList = aResponse.AuctionList.filterByOwnerNames(aRequest.OwnerFilters)
		}
		if len(aRequest.ItemFilters) > 0 {
			aResponse.AuctionList = aResponse.AuctionList.filterByItemIDs(aRequest.ItemFilters)
		}

		// calculating the total for paging
		aResponse.Total = len(aResponse.AuctionList)

		// calculating the total-count for review
		totalCount := 0
		for _, mAuction := range realmAuctions {
			totalCount += len(mAuction.AucList)
		}
		aResponse.TotalCount = totalCount

		// optionally sorting
		if aRequest.SortKind != sortkinds.None && aRequest.SortDirection != sortdirections.None {
			err = aResponse.AuctionList.sort(aRequest.SortKind, aRequest.SortDirection)
			if err != nil {
				m.Err = err.Error()
				m.Code = codes.UserError
				sta.messenger.replyTo(natsMsg, m)

				return
			}
		}

		// truncating the list
		aResponse.AuctionList, err = aResponse.AuctionList.limit(aRequest.Count, aRequest.Page)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.UserError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		// appending item names where appropriate
		aResponse.AuctionList = aResponse.AuctionList.appendItemNames(sta.items)

		// encoding the auctions list for output
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
