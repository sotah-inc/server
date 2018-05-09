package main

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
	"github.com/ihsw/sotah-server/app/util"
)

func newOwnersFromAuctions(aucs miniAuctionList) owners {
	ownerNamesMap := map[string]struct{}{}
	for _, ma := range aucs {
		ownerNamesMap[ma.Owner] = struct{}{}
	}

	ownerList := make([]owner, len(ownerNamesMap))
	i := 0
	for ownerName := range ownerNamesMap {
		ownerList[i] = owner{ownerName}
		i++
	}

	return owners{Owners: ownerList}
}

func newOwnersFromMessenger(mess messenger, request ownersRequest) (*owners, error) {
	encodedMessage, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	msg, err := mess.request(subjects.Owners, encodedMessage)
	if err != nil {
		return nil, err
	}

	if msg.Code != codes.Ok {
		return nil, errors.New(msg.Err)
	}

	return newOwners([]byte(msg.Data))
}

func newOwnersFromFilepath(relativeFilepath string) (*owners, error) {
	body, err := util.ReadFile(relativeFilepath)
	if err != nil {
		return nil, err
	}

	return newOwners(body)
}

func newOwners(payload []byte) (*owners, error) {
	o := &owners{}
	if err := json.Unmarshal(payload, &o); err != nil {
		return nil, err
	}

	return o, nil
}

type owners struct {
	Owners ownersList `json:"owners"`
}

type ownersList []owner

type owner struct {
	Name string `json:"name"`
}

func (ol ownersList) limit() ownersList {
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

func (ol ownersList) filter(query string) ownersList {
	lowerQuery := strings.ToLower(query)
	matches := ownersList{}
	for _, o := range ol {
		if !strings.Contains(strings.ToLower(o.Name), lowerQuery) {
			continue
		}

		matches = append(matches, o)
	}

	return matches
}

type ownersByName ownersList

func (by ownersByName) Len() int           { return len(by) }
func (by ownersByName) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by ownersByName) Less(i, j int) bool { return by[i].Name < by[j].Name }

// use this: https://godoc.org/github.com/renstrom/fuzzysearch/fuzzy
