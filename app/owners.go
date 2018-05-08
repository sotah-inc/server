package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"

	"github.com/ihsw/sotah-server/app/codes"
	"github.com/ihsw/sotah-server/app/subjects"
	"github.com/ihsw/sotah-server/app/util"
)

func newOwnersFromAuctions(aucs miniAuctionList) owners {
	ownersMap := map[string]struct{}{}
	for _, ma := range aucs {
		ownersMap[ma.Owner] = struct{}{}
	}

	ownerList := make([]string, len(ownersMap))
	i := 0
	for owner := range ownersMap {
		ownerList[i] = owner
		i++
	}

	return owners{Owners: ownerList}
}

func newOwnersFromMessenger(mess messenger, reg region, rea realm) (*owners, error) {
	request := ownersRequest{reg.Name, rea.Slug}
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

	return newOwnersFromEncoded([]byte(msg.Data))
}

func newOwnersFromEncoded(body []byte) (*owners, error) {
	base64Decoded, err := base64.StdEncoding.DecodeString(string(body))
	if err != nil {
		return nil, err
	}

	gzipDecoded, err := util.GzipDecode(base64Decoded)
	if err != nil {
		return nil, err
	}

	return newOwners(gzipDecoded)
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

func (o owners) encodeForMessage() (string, error) {
	jsonEncoded, err := json.Marshal(o)
	if err != nil {
		return "", err
	}

	gzipEncodedAuctions, err := util.GzipEncode(jsonEncoded)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(gzipEncodedAuctions), nil
}

type ownersList []string

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

type ownersByName ownersList

func (by ownersByName) Len() int           { return len(by) }
func (by ownersByName) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by ownersByName) Less(i, j int) bool { return by[i] < by[j] }

// use this: https://godoc.org/github.com/renstrom/fuzzysearch/fuzzy
