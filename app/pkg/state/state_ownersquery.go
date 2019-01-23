package state

import (
	"encoding/json"
	"errors"
	"sort"

	nats "github.com/nats-io/go-nats"
	"github.com/renstrom/fuzzysearch/fuzzy"
	"github.com/sotah-inc/server/app/blizzard"
	"github.com/sotah-inc/server/app/codes"
	"github.com/sotah-inc/server/app/subjects"
)

type ownersQueryItem struct {
	Target string `json:"target"`
	Owner  owner  `json:"owner"`
	Rank   int    `json:"rank"`
}

type ownersQueryItems []ownersQueryItem

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
	Items ownersQueryItems `json:"items"`
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

func (request ownersQueryRequest) resolve(sta State) (ownersQueryResult, error) {
	if request.RegionName == "" {
		return ownersQueryResult{}, errors.New("Region name cannot be blank")
	}
	if request.RealmSlug == "" {
		return ownersQueryResult{}, errors.New("Realm slug cannot be blank")
	}

	// resolving region-Realm auctions
	regionLadBases, ok := sta.liveAuctionsDatabases[request.RegionName]
	if !ok {
		return ownersQueryResult{}, errors.New("Invalid region name")
	}

	ladBase, ok := regionLadBases[request.RealmSlug]
	if !ok {
		return ownersQueryResult{}, errors.New("Invalid Realm slug")
	}

	maList, err := ladBase.getMiniauctions()
	if err != nil {
		return ownersQueryResult{}, err
	}

	// resolving owners from auctions
	oResult, err := newOwnersFromAuctions(maList)
	if err != nil {
		return ownersQueryResult{}, err
	}

	// formatting owners and items into an auctions-query result
	oqResult := ownersQueryResult{
		Items: make(ownersQueryItems, len(oResult.Owners)),
	}
	i := 0
	for _, ownerValue := range oResult.Owners {
		oqResult.Items[i] = ownersQueryItem{
			Owner:  ownerValue,
			Target: ownerValue.NormalizedName,
		}
		i++
	}

	return oqResult, nil
}

func (sta State) listenForOwnersQuery(stop ListenStopChan) error {
	err := sta.Messenger.subscribe(subjects.OwnersQuery, stop, func(natsMsg nats.Msg) {
		m := newMessage()

		// resolving the request
		request, err := newOwnersQueryRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.Messenger.replyTo(natsMsg, m)

			return
		}

		// resolving result from the request and State
		result, err := request.resolve(sta)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.NotFound
			sta.Messenger.replyTo(natsMsg, m)

			return
		}

		// optionally sorting by rank and truncating or sorting by name
		if request.Query != "" {
			for i, oqItem := range result.Items {
				oqItem.Rank = fuzzy.RankMatchFold(request.Query, oqItem.Target)
				result.Items[i] = oqItem
			}
			result.Items = result.Items.filterLowRank()
			sort.Sort(ownersQueryItemsByRank(result.Items))
		} else {
			sort.Sort(ownersQueryItemsByNames(result.Items))
		}

		// truncating
		result.Items = result.Items.limit()

		// marshalling for Messenger
		encodedMessage, err := json.Marshal(result)
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
