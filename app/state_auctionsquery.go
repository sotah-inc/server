package main

import (
	"encoding/json"
	"errors"
	"sort"

	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
	nats "github.com/nats-io/go-nats"
)

type auctionsQueryItem struct {
	Target string `json:"target"`
	Item   item   `json:"item"`
	Owner  owner  `json:"owner"`
}

type auctionsQueryItems []auctionsQueryItem

func (aqItems auctionsQueryItems) limit() auctionsQueryItems {
	listLength := len(aqItems)
	if listLength > 10 {
		listLength = 10
	}

	out := make(auctionsQueryItems, listLength)
	for i := 0; i < listLength; i++ {
		out[i] = aqItems[i]
	}

	return out
}

type auctionsQueryItemsByNames auctionsQueryItems

func (by auctionsQueryItemsByNames) Len() int           { return len(by) }
func (by auctionsQueryItemsByNames) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by auctionsQueryItemsByNames) Less(i, j int) bool { return by[i].Target < by[j].Target }

func newAuctionsQueryResultFromMessenger(mess messenger, request auctionsQueryRequest) (auctionsQueryResult, error) {
	encodedMessage, err := json.Marshal(request)
	if err != nil {
		return auctionsQueryResult{}, err
	}

	msg, err := mess.request(subjects.AuctionsQuery, encodedMessage)
	if err != nil {
		return auctionsQueryResult{}, err
	}

	if msg.Code != codes.Ok {
		return auctionsQueryResult{}, errors.New(msg.Err)
	}

	return newAuctionsQueryResult([]byte(msg.Data))
}

func newAuctionsQueryResult(payload []byte) (auctionsQueryResult, error) {
	result := &auctionsQueryResult{}
	err := json.Unmarshal(payload, result)
	if err != nil {
		return auctionsQueryResult{}, err
	}

	return *result, nil
}

type auctionsQueryResult struct {
	Items auctionsQueryItems `json:"items"`
}

func newAuctionsQueryRequest(payload []byte) (auctionsQueryRequest, error) {
	request := &auctionsQueryRequest{}
	err := json.Unmarshal(payload, &request)
	if err != nil {
		return auctionsQueryRequest{}, err
	}

	return *request, nil
}

type auctionsQueryRequest struct {
	RegionName regionName `json:"region_name"`
	RealmSlug  realmSlug  `json:"realm_slug"`
	Query      string     `json:"query"`
}

func (request auctionsQueryRequest) resolve(sta state) (auctionsQueryResult, error) {
	if request.RegionName == "" {
		return auctionsQueryResult{}, errors.New("Region name cannot be blank")
	}
	if request.RealmSlug == "" {
		return auctionsQueryResult{}, errors.New("Realm slug cannot be blank")
	}

	// resolving region-realm auctions
	regionAuctions, ok := sta.auctions[request.RegionName]
	if !ok {
		return auctionsQueryResult{}, errors.New("Invalid region name")
	}
	realmAuctions, ok := regionAuctions[request.RealmSlug]
	if !ok {
		return auctionsQueryResult{}, errors.New("Invalid realm slug")
	}

	// resolving owners from auctions
	oResult, err := newOwnersFromAuctions(realmAuctions)
	if err != nil {
		return auctionsQueryResult{}, err
	}

	// resolving items
	ilResult := itemListResult{Items: itemList{}}
	for _, itemValue := range sta.items {
		ilResult.Items = append(ilResult.Items, itemValue)
	}

	// formatting owners and items into an auctions-query result
	aqResult := auctionsQueryResult{
		Items: make(auctionsQueryItems, len(oResult.Owners)+len(ilResult.Items)),
	}
	i := 0
	for _, ownerValue := range oResult.Owners {
		aqResult.Items[i] = auctionsQueryItem{Owner: ownerValue, Item: item{}, Target: ownerValue.NormalizedName}
		i++
	}
	for _, itemValue := range ilResult.Items {
		aqResult.Items[i] = auctionsQueryItem{Owner: owner{}, Item: itemValue, Target: itemValue.NormalizedName}
		i++
	}

	return aqResult, nil
}

func (sta state) listenForAuctionsQuery(stop chan interface{}) error {
	err := sta.messenger.subscribe(subjects.AuctionsQuery, stop, func(natsMsg nats.Msg) {
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

		// sorting and truncating the list of items
		sort.Sort(auctionsQueryItemsByNames(aqResult.Items))
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
