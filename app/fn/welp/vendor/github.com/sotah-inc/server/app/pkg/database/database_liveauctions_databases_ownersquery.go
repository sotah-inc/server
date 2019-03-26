package database

import (
	"encoding/json"
	"errors"
	"sort"

	"github.com/lithammer/fuzzysearch/fuzzy"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/database/codes"
	"github.com/sotah-inc/server/app/pkg/sotah"
)

func NewQueryOwnersRequest(data []byte) (QueryOwnersRequest, error) {
	var out QueryOwnersRequest
	if err := json.Unmarshal(data, &out); err != nil {
		return QueryOwnersRequest{}, err
	}

	return out, nil
}

type QueryOwnersRequest struct {
	RegionName blizzard.RegionName `json:"region_name"`
	RealmSlug  blizzard.RealmSlug  `json:"realm_slug"`
	Query      string              `json:"query"`
}

type QueryOwnersItem struct {
	Target string      `json:"target"`
	Owner  sotah.Owner `json:"owner"`
	Rank   int         `json:"rank"`
}

type QueryOwnersItems []QueryOwnersItem

func (items QueryOwnersItems) Limit() QueryOwnersItems {
	listLength := len(items)
	if listLength > 10 {
		listLength = 10
	}

	out := make(QueryOwnersItems, listLength)
	for i := 0; i < listLength; i++ {
		out[i] = items[i]
	}

	return out
}

func (items QueryOwnersItems) FilterLowRank() QueryOwnersItems {
	out := QueryOwnersItems{}
	for _, itemValue := range items {
		if itemValue.Rank == -1 {
			continue
		}
		out = append(out, itemValue)
	}

	return out
}

type QueryOwnersItemsByNames QueryOwnersItems

func (by QueryOwnersItemsByNames) Len() int           { return len(by) }
func (by QueryOwnersItemsByNames) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by QueryOwnersItemsByNames) Less(i, j int) bool { return by[i].Target < by[j].Target }

type QueryOwnersItemsByRank QueryOwnersItems

func (by QueryOwnersItemsByRank) Len() int           { return len(by) }
func (by QueryOwnersItemsByRank) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by QueryOwnersItemsByRank) Less(i, j int) bool { return by[i].Rank < by[j].Rank }

type QueryOwnersResponse struct {
	Items QueryOwnersItems `json:"items"`
}

func (r QueryOwnersResponse) EncodeForDelivery() ([]byte, error) {
	return json.Marshal(r)
}

func (ladBases LiveAuctionsDatabases) QueryOwners(qr QueryOwnersRequest) (QueryOwnersResponse, codes.Code, error) {
	regionLadBases, ok := ladBases[qr.RegionName]
	if !ok {
		return QueryOwnersResponse{}, codes.UserError, errors.New("invalid region")
	}

	realmLadbase, ok := regionLadBases[qr.RealmSlug]
	if !ok {
		return QueryOwnersResponse{}, codes.UserError, errors.New("invalid realm")
	}

	maList, err := realmLadbase.GetMiniAuctionList()
	if err != nil {
		return QueryOwnersResponse{}, codes.GenericError, err
	}

	// resolving owners from auctions
	owners, err := sotah.NewOwnersFromAuctions(maList)
	if err != nil {
		return QueryOwnersResponse{}, codes.GenericError, err
	}

	// formatting owners and items into an owners-query result
	response := QueryOwnersResponse{Items: QueryOwnersItems{}}
	for _, ownerValue := range owners.Owners {
		response.Items = append(response.Items, QueryOwnersItem{
			Owner:  ownerValue,
			Target: ownerValue.NormalizedName,
		})
	}

	// optionally sorting by rank and truncating or sorting by name
	if qr.Query != "" {
		for i, oqItem := range response.Items {
			oqItem.Rank = fuzzy.RankMatchFold(qr.Query, oqItem.Target)
			response.Items[i] = oqItem
		}

		response.Items = response.Items.FilterLowRank()
		sort.Sort(QueryOwnersItemsByRank(response.Items))
	} else {
		sort.Sort(QueryOwnersItemsByNames(response.Items))
	}

	response.Items = response.Items.Limit()

	return response, codes.Ok, nil
}
