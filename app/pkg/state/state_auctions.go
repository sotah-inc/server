package state

import (
	"encoding/base64"
	"encoding/json"
	"errors"

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
	OwnerFilters  []sotah.OwnerName            `json:"owner_filters"`
	ItemFilters   []blizzard.ItemID            `json:"item_filters"`
}

func (ar AuctionsRequest) resolve(sta State) (sotah.MiniAuctionList, requestError) {
	regionLadBases, ok := sta.IO.databases.LiveAuctionsDatabases[ar.RegionName]
	if !ok {
		return sotah.MiniAuctionList{}, requestError{codes.NotFound, "Invalid region"}
	}

	realmLadbase, ok := regionLadBases[ar.RealmSlug]
	if !ok {
		return sotah.MiniAuctionList{}, requestError{codes.NotFound, "Invalid Realm"}
	}

	if ar.Page < 0 {
		return sotah.MiniAuctionList{}, requestError{codes.UserError, "Page must be >=0"}
	}
	if ar.Count == 0 {
		return sotah.MiniAuctionList{}, requestError{codes.UserError, "Count must be >0"}
	} else if ar.Count > 1000 {
		return sotah.MiniAuctionList{}, requestError{codes.UserError, "Count must be <=1000"}
	}

	maList, err := realmLadbase.GetMiniauctions()
	if err != nil {
		return sotah.MiniAuctionList{}, requestError{codes.GenericError, err.Error()}
	}

	return maList, requestError{codes.Ok, ""}
}

type auctionsResponse struct {
	AuctionList sotah.MiniAuctionList `json:"auctions"`
	Total       int                   `json:"total"`
	TotalCount  int                   `json:"total_count"`
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

func (sta State) ListenForAuctions(stop messenger.ListenStopChan) error {
	err := sta.IO.messenger.Subscribe(subjects.Auctions, stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		// resolving the request
		aRequest, err := newAuctionsRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.IO.messenger.ReplyTo(natsMsg, m)

			return
		}

		// resolving data from State
		realmAuctions, reErr := aRequest.resolve(sta)
		if reErr.code != codes.Ok {
			m.Err = reErr.message
			m.Code = reErr.code
			sta.IO.messenger.ReplyTo(natsMsg, m)

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
				sta.IO.messenger.ReplyTo(natsMsg, m)

				return
			}
		}

		// truncating the list
		aResponse.AuctionList, err = aResponse.AuctionList.limit(aRequest.Count, aRequest.Page)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.UserError
			sta.IO.messenger.ReplyTo(natsMsg, m)

			return
		}

		// encoding the auctions list for output
		data, err := aResponse.encodeForMessage()
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.IO.messenger.ReplyTo(natsMsg, m)

			return
		}

		m.Data = data
		sta.IO.messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}

type NewMiniAuctionsListFromMessengerConfig struct {
	realm         sotah.Realm
	count         int
	page          int
	sortDirection sortdirections.SortDirection
	sortKind      sortkinds.SortKind
	ownerFilter   sotah.OwnerName
}

func (sta State) NewMiniAuctionsList(req AuctionsRequest) (sotah.MiniAuctionList, error) {
	encodedMessage, err := json.Marshal(req)
	if err != nil {
		return sotah.MiniAuctionList{}, err
	}

	msg, err := sta.IO.messenger.Request(subjects.Auctions, encodedMessage)
	if err != nil {
		return sotah.MiniAuctionList{}, err
	}

	if msg.Code != codes.Ok {
		return sotah.MiniAuctionList{}, errors.New(msg.Err)
	}

	return sotah.NewMiniAuctionListFromGzipped([]byte(msg.Data))
}
