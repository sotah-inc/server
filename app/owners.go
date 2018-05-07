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

func newOwnersFromMessenger(reg region, rea realm, mess messenger) (*owners, error) {
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

	return newOwners([]byte(msg.Data))
}

func newOwners(payload []byte) (*owners, error) {
	o := &owners{}
	if err := json.Unmarshal(payload, &o); err != nil {
		return nil, err
	}

	return o, nil
}

type owners struct {
	Owners []string `json:"owners"`
}

func (o owners) encodeForMessage() (string, error) {
	jsonEncoded, err := json.Marshal(o.Owners)
	if err != nil {
		return "", err
	}

	gzipEncodedAuctions, err := util.GzipEncode(jsonEncoded)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(gzipEncodedAuctions), nil
}
