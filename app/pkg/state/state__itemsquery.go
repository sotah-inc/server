package state

import (
	"encoding/json"
	"sort"

	"github.com/lithammer/fuzzysearch/fuzzy"
	nats "github.com/nats-io/go-nats"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
	"github.com/sotah-inc/server/app/pkg/sotah"
)

type itemsQueryItem struct {
	Target string     `json:"target"`
	Item   sotah.Item `json:"item"`
	Rank   int        `json:"rank"`
}

type itemsQueryItems []itemsQueryItem

func (iqItems itemsQueryItems) limit() itemsQueryItems {
	listLength := len(iqItems)
	if listLength > 10 {
		listLength = 10
	}

	out := make(itemsQueryItems, listLength)
	for i := 0; i < listLength; i++ {
		out[i] = iqItems[i]
	}

	return out
}

func (iqItems itemsQueryItems) filterLowRank() itemsQueryItems {
	out := itemsQueryItems{}
	for _, itemValue := range iqItems {
		if itemValue.Rank == -1 {
			continue
		}
		out = append(out, itemValue)
	}

	return out
}

type itemsQueryItemsByTarget itemsQueryItems

func (by itemsQueryItemsByTarget) Len() int           { return len(by) }
func (by itemsQueryItemsByTarget) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by itemsQueryItemsByTarget) Less(i, j int) bool { return by[i].Target < by[j].Target }

type itemsQueryItemsByRank itemsQueryItems

func (by itemsQueryItemsByRank) Len() int           { return len(by) }
func (by itemsQueryItemsByRank) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by itemsQueryItemsByRank) Less(i, j int) bool { return by[i].Rank < by[j].Rank }

type itemsQueryResult struct {
	Items itemsQueryItems `json:"items"`
}

func newItemsQueryRequest(payload []byte) (itemsQueryRequest, error) {
	request := &itemsQueryRequest{}
	err := json.Unmarshal(payload, &request)
	if err != nil {
		return itemsQueryRequest{}, err
	}

	return *request, nil
}

type itemsQueryRequest struct {
	Query string `json:"query"`
}

func (request itemsQueryRequest) resolve(sta State) (itemsQueryResult, error) {
	iMap, err := sta.IO.Databases.ItemsDatabase.GetItems()
	if err != nil {
		return itemsQueryResult{}, err
	}

	iqResult := itemsQueryResult{
		Items: make(itemsQueryItems, len(iMap)),
	}
	i := 0
	for _, itemValue := range iMap {
		iqResult.Items[i] = itemsQueryItem{Item: itemValue, Target: itemValue.NormalizedName}
		i++
	}

	return iqResult, nil
}

func (sta State) ListenForItemsQuery(stop messenger.ListenStopChan) error {
	err := sta.IO.Messenger.Subscribe(subjects.ItemsQuery, stop, func(natsMsg nats.Msg) {
		m := messenger.NewMessage()

		// resolving the request
		request, err := newItemsQueryRequest(natsMsg.Data)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.MsgJSONParseError
			sta.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		// resolving the items-query result
		iqResult, err := request.resolve(sta)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		// optionally sorting by rank or sorting by name
		if request.Query != "" {
			for i, iqItem := range iqResult.Items {
				iqItem.Rank = fuzzy.RankMatchFold(request.Query, iqItem.Target)
				iqResult.Items[i] = iqItem
			}
			iqResult.Items = iqResult.Items.filterLowRank()
			sort.Sort(itemsQueryItemsByRank(iqResult.Items))
		} else {
			sort.Sort(itemsQueryItemsByTarget(iqResult.Items))
		}

		// truncating
		iqResult.Items = iqResult.Items.limit()

		// marshalling for Messenger
		encodedMessage, err := json.Marshal(iqResult)
		if err != nil {
			m.Err = err.Error()
			m.Code = codes.GenericError
			sta.IO.Messenger.ReplyTo(natsMsg, m)

			return
		}

		// dumping it out
		m.Data = string(encodedMessage)
		sta.IO.Messenger.ReplyTo(natsMsg, m)
	})
	if err != nil {
		return err
	}

	return nil
}
