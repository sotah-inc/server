package main

import (
	"encoding/json"
	"errors"
	"sort"

	"github.com/ihsw/sotah-server/app/blizzard"
	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
	nats "github.com/nats-io/go-nats"
	"github.com/renstrom/fuzzysearch/fuzzy"
)

type ownersQueryItem struct {
	Target string `json:"target"`
	Item   item   `json:"item"`
	Owner  owner  `json:"owner"`
	Rank   int    `json:"rank"`
}

type ownersQueryItems []auctionsQueryItem

func (aqItems ownersQueryItems) limit() ownersQueryItems {
	listLength := len(aqItems)
	if listLength > 10 {
		listLength = 10
	}

	out := make(ownersQueryItems, listLength)
	for i := 0; i < listLength; i++ {
		out[i] = aqItems[i]
	}

	return out
}

func (aqItems ownersQueryItems) filterLowRank() ownersQueryItems {
	out := ownersQueryItems{}
	for _, itemValue := range aqItems {
		if itemValue.Rank == -1 {
			continue
		}
		out = append(out, itemValue)
	}

	return out
}

type ownersQueryItemsByNames ownersQueryItems

func (by ownersQueryItemsByNames) Len() int           { return len(by) }
func (by ownersQueryItemsByNames) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by ownersQueryItemsByNames) Less(i, j int) bool { return by[i].Target < by[j].Target }

type ownersQueryItemsByRank ownersQueryItems

func (by ownersQueryItemsByRank) Len() int           { return len(by) }
func (by ownersQueryItemsByRank) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by ownersQueryItemsByRank) Less(i, j int) bool { return by[i].Rank < by[j].Rank }

func newOwnersQueryResult(payload []byte) (ownersQueryResult, error) {
	result := &ownersQueryResult{}
	err := json.Unmarshal(payload, result)
	if err != nil {
		return ownersQueryResult{}, err
	}

	return *result, nil
}

type ownersQueryResult struct {
	Items auctionsQueryItems `json:"items"`
}

func newOwnersQueryRequest(payload []byte) (ownersQueryRequest, error) {
	request := &ownersQueryRequest{}
	err := json.Unmarshal(payload, &request)
	if err != nil {
		return ownersQueryRequest{}, err
	}

	return *request, nil
}

type ownersQueryRequest struct {
	RegionName regionName         `json:"region_name"`
	RealmSlug  blizzard.RealmSlug `json:"realm_slug"`
	Query      string             `json:"query"`
}

func (request ownersQueryRequest) resolve(sta state) (ownersQueryResult, error) {
	if request.RegionName == "" {
		return ownersQueryResult{}, errors.New("Region name cannot be blank")
	}
	if request.RealmSlug == "" {
		return ownersQueryResult{}, errors.New("Realm slug cannot be blank")
	}

	// resolving region-realm auctions
	regionAuctions, ok := sta.auctions[request.RegionName]
	if !ok {
		return ownersQueryResult{}, errors.New("Invalid region name")
	}
	realmAuctions, ok := regionAuctions[request.RealmSlug]
	if !ok {
		return ownersQueryResult{}, errors.New("Invalid realm slug")
	}

	// resolving owners from auctions
	oResult, err := newOwnersFromAuctions(realmAuctions)
	if err != nil {
		return ownersQueryResult{}, err
	}

	// resolving items
	iqResult := itemsQueryResult{Items: itemsQueryItems{}}
	for _, itemValue := range sta.items {
		iqResult.Items = append(
			iqResult.Items,
			itemsQueryItem{Item: itemValue},
		)
	}

	// formatting owners and items into an auctions-query result
	aqResult := ownersQueryResult{
		Items: make(auctionsQueryItems, len(oResult.Owners)+len(iqResult.Items)),
	}
	i := 0
	for _, ownerValue := range oResult.Owners {
		aqResult.Items[i] = auctionsQueryItem{
			Owner:  ownerValue,
			Item:   item{},
			Target: ownerValue.NormalizedName,
		}
		i++
	}
	for _, iqItem := range iqResult.Items {
		aqResult.Items[i] = auctionsQueryItem{
			Owner:  owner{},
			Item:   iqItem.Item,
			Target: iqItem.Item.NormalizedName,
		}
		i++
	}

	return aqResult, nil
}

func (sta state) listenForOwnersQuery(stop listenStopChan) error {
	err := sta.messenger.subscribe(subjects.OwnersQuery, stop, func(natsMsg nats.Msg) {
		m := newMessage()

		// resolving the request
		request, err := newAuctionsQueryRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		// resolving result from the request and state
		aqResult, err := request.resolve(sta)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.NotFound
			sta.messenger.replyTo(natsMsg, m)

			return
		}

		// optionally sorting by rank and truncating or sorting by name
		if request.Query != "" {
			for i, aqItem := range aqResult.Items {
				aqItem.Rank = fuzzy.RankMatchFold(request.Query, aqItem.Target)
				aqResult.Items[i] = aqItem
			}
			aqResult.Items = aqResult.Items.filterLowRank()
			sort.Sort(auctionsQueryItemsByRank(aqResult.Items))
		} else {
			sort.Sort(auctionsQueryItemsByNames(aqResult.Items))
		}

		// truncating
		aqResult.Items = aqResult.Items.limit()

		// marshalling for messenger
		encodedMessage, err := json.Marshal(aqResult)
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
