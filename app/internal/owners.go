package internal

import (
	"encoding/json"
	"errors"
	"regexp"
	"strings"

	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/codes"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
	"github.com/sotah-inc/server/app/pkg/state"
	"github.com/sotah-inc/server/app/pkg/util"
)

type OwnerName string

type Owner struct {
	Name           OwnerName `json:"name"`
	NormalizedName string    `json:"normalized_name"`
}

func NewOwnersFromAuctions(aucs MiniAuctionList) (owners, error) {
	ownerNamesMap := map[OwnerName]struct{}{}
	for _, ma := range aucs {
		ownerNamesMap[ma.Owner] = struct{}{}
	}

	reg, err := regexp.Compile("[^a-z0-9 ]+")
	if err != nil {
		return owners{}, err
	}

	ownerList := make([]Owner, len(ownerNamesMap))
	i := 0
	for ownerNameValue := range ownerNamesMap {
		ownerList[i] = Owner{
			Name:           ownerNameValue,
			NormalizedName: reg.ReplaceAllString(strings.ToLower(string(ownerNameValue)), ""),
		}
		i++
	}

	return owners{Owners: ownerList}, nil
}

func newOwnersFromMessenger(mess messenger.Messenger, request state.OwnersRequest) (owners, error) {
	encodedMessage, err := json.Marshal(request)
	if err != nil {
		return owners{}, err
	}

	msg, err := mess.Request(subjects.Owners, encodedMessage)
	if err != nil {
		return owners{}, err
	}

	if msg.Code != codes.Ok {
		return owners{}, errors.New(msg.Err)
	}

	return newOwners([]byte(msg.Data))
}

func newOwnersFromFilepath(relativeFilepath string) (owners, error) {
	body, err := util.ReadFile(relativeFilepath)
	if err != nil {
		return owners{}, err
	}

	return newOwners(body)
}

func newOwners(payload []byte) (owners, error) {
	o := &owners{}
	if err := json.Unmarshal(payload, &o); err != nil {
		return owners{}, err
	}

	return *o, nil
}

type owners struct {
	Owners ownersList `json:"owners"`
}

type ownersList []Owner

func (ol ownersList) Limit() ownersList {
	listLength := len(ol)
	if listLength > 10 {
		listLength = 10
	}

	out := make(ownersList, listLength)
	for i := 0; i < listLength; i++ {
		out[i] = ol[i]
	}

	return out
}

func (ol ownersList) Filter(query string) ownersList {
	lowerQuery := strings.ToLower(query)
	matches := ownersList{}
	for _, o := range ol {
		if !strings.Contains(strings.ToLower(string(o.Name)), lowerQuery) {
			continue
		}

		matches = append(matches, o)
	}

	return matches
}

type OwnersByName ownersList

func (by OwnersByName) Len() int           { return len(by) }
func (by OwnersByName) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by OwnersByName) Less(i, j int) bool { return by[i].Name < by[j].Name }

// use this: https://godoc.org/github.com/lithammer/fuzzysearch/fuzzy
