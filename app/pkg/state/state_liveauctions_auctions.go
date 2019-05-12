package state

import (
	"encoding/base64"
	"encoding/json"
	"errors"

	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/sotah/sortdirections"
	"github.com/sotah-inc/server/app/pkg/sotah/sortkinds"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
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

func (ar AuctionsRequest) resolve(laState LiveAuctionsState) (sotah.MiniAuctionList, requestError) {
	regionLadBases, ok := laState.IO.Databases.LiveAuctionsDatabases[ar.RegionName]
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

	maList, err := realmLadbase.GetMiniAuctionList()
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

func (laState LiveAuctionsState) ListenForAuctions(stop ListenStopChan) error {
	err := laState.IO.Messenger.Subscribe(string(subjects.Auctions), stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		// resolving the request
		aRequest, err := newAuctionsRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			laState.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		// resolving data from State
		realmAuctions, reErr := aRequest.resolve(laState)
		if reErr.code != codes.Ok {
			m.Err = reErr.message
			m.Code = reErr.code
			laState.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		// initial response format
		aResponse := auctionsResponse{Total: -1, TotalCount: -1, AuctionList: realmAuctions}

		// filtering in auctions by owners or items
		if len(aRequest.OwnerFilters) > 0 {
			aResponse.AuctionList = aResponse.AuctionList.FilterByOwnerNames(aRequest.OwnerFilters)
		}
		if len(aRequest.ItemFilters) > 0 {
			aResponse.AuctionList = aResponse.AuctionList.FilterByItemIDs(aRequest.ItemFilters)
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
			err = aResponse.AuctionList.Sort(aRequest.SortKind, aRequest.SortDirection)
			if err != nil {
				m.Err = err.Error()
				m.Code = codes.UserError
				laState.IO.Messenger.ReplyTo(natsMsg, m)

				return
			}
		}

		// truncating the list
		aResponse.AuctionList, err = aResponse.AuctionList.Limit(aRequest.Count, aRequest.Page)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.UserError
			laState.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		// encoding the auctions list for output
		data, err := aResponse.encodeForMessage()
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			laState.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		m.Data = data
		laState.IO.Messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}

func (sta State) NewMiniAuctionsList(req AuctionsRequest) (sotah.MiniAuctionList, error) {
	encodedMessage, err := json.Marshal(req)
	if err != nil {
		return sotah.MiniAuctionList{}, err
	}

	msg, err := sta.IO.Messenger.Request(string(subjects.Auctions), encodedMessage)
	if err != nil {
		return sotah.MiniAuctionList{}, err
	}

	if msg.Code != codes.Ok {
		return sotah.MiniAuctionList{}, errors.New(msg.Err)
	}

	return sotah.NewMiniAuctionListFromGzipped([]byte(msg.Data))
}
