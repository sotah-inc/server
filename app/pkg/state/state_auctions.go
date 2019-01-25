package state

import (
	"encoding/base64"
	"encoding/json"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/sortdirections"
	"github.com/sotah-inc/server/app/pkg/state/sortkinds"
	"github.com/sotah-inc/server/app/pkg/util"
)

func newAuctionsRequest(payload []byte) (AuctionsRequest, error) {
	ar := &AuctionsRequest{}
	err := json.Unmarshal(payload, &ar)
	if err != nil {
		return AuctionsRequest{}, err
	}

	return *ar, nil
}

type AuctionsRequest struct {
	RegionName    blizzard.RegionName          `json:"region_name"`
	RealmSlug     blizzard.RealmSlug           `json:"realm_slug"`
	Page          int                          `json:"page"`
	Count         int                          `json:"count"`
	SortDirection sortdirections.SortDirection `json:"sort_direction"`
	SortKind      sortkinds.SortKind           `json:"sort_kind"`
	OwnerFilters  []ownerName                  `json:"owner_filters"`
	ItemFilters   []blizzard.ItemID            `json:"item_filters"`
}

func (ar AuctionsRequest) resolve(sta State) (miniAuctionList, requestError) {
	regionLadBases, ok := sta.LiveAuctionsDatabases[ar.RegionName]
	if !ok {
		return miniAuctionList{}, requestError{codes.NotFound, "Invalid region"}
	}

	realmLadbase, ok := regionLadBases[ar.RealmSlug]
	if !ok {
		return miniAuctionList{}, requestError{codes.NotFound, "Invalid Realm"}
	}

	if ar.Page < 0 {
		return miniAuctionList{}, requestError{codes.UserError, "Page must be >=0"}
	}
	if ar.Count == 0 {
		return miniAuctionList{}, requestError{codes.UserError, "Count must be >0"}
	} else if ar.Count > 1000 {
		return miniAuctionList{}, requestError{codes.UserError, "Count must be <=1000"}
	}

	maList, err := realmLadbase.getMiniauctions()
	if err != nil {
		return miniAuctionList{}, requestError{codes.GenericError, err.Error()}
	}

	return maList, requestError{codes.Ok, ""}
}

func NewAuctionsResponseFromEncoded(body []byte) (auctionsResponse, error) {
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

func (sta State) ListenForAuctions(stop ListenStopChan) error {
	err := sta.Messenger.Subscribe(subjects.Auctions, stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		// resolving the request
		aRequest, err := newAuctionsRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.Messenger.ReplyTo(natsMsg, m)

			return
		}

		// resolving data from State
		realmAuctions, reErr := aRequest.resolve(sta)
		if reErr.code != codes.Ok {
			m.Err = reErr.message
			m.Code = reErr.code
			sta.Messenger.ReplyTo(natsMsg, m)

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
				sta.Messenger.ReplyTo(natsMsg, m)

				return
			}
		}

		// truncating the list
		aResponse.AuctionList, err = aResponse.AuctionList.limit(aRequest.Count, aRequest.Page)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.UserError
			sta.Messenger.ReplyTo(natsMsg, m)

			return
		}

		// encoding the auctions list for output
		data, err := aResponse.encodeForMessage()
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.Messenger.ReplyTo(natsMsg, m)

			return
		}

		m.Data = data
		sta.Messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
