package main

import "github.com/ihsw/sotah-server/app/util"
import "encoding/json"

type getAuctionsWhitelist map[realmSlug]interface{}

type getAuctionsJob struct {
	err      error
	realm    realm
	auctions *auctions
}

type realms []realm

func (reas realms) getAllAuctions(res resolver) chan getAuctionsJob {
	return reas.getAuctions(res, map[realmSlug]interface{}{})
}

func (reas realms) getAuctions(res resolver, whitelist getAuctionsWhitelist) chan getAuctionsJob {
	// establishing channels
	out := make(chan getAuctionsJob)
	in := make(chan realm)

	// spinning up the workers for fetching auctions
	worker := func() {
		for rea := range in {
			aucs, err := rea.getAuctions(res)
			out <- getAuctionsJob{err: err, realm: rea, auctions: aucs}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(4, worker, postWork)

	// queueing up the realms
	go func() {
		for _, rea := range reas {
			if _, ok := whitelist[rea.Slug]; !ok {
				continue
			}

			in <- rea
		}

		close(in)
	}()

	return out
}

func newRealmFromFilepath(reg region, relativeFilepath string) (*realm, error) {
	body, err := util.ReadFile(relativeFilepath)
	if err != nil {
		return nil, err
	}

	return newRealm(reg, body)
}

func newRealm(reg region, body []byte) (*realm, error) {
	rea := &realm{}
	if err := json.Unmarshal(body, &rea); err != nil {
		return nil, err
	}

	rea.region = reg
	return rea, nil
}

type realmSlug string

type realm struct {
	Type            string      `json:"type"`
	Population      string      `json:"population"`
	Queue           bool        `json:"queue"`
	Status          bool        `json:"status"`
	Name            string      `json:"name"`
	Slug            realmSlug   `json:"slug"`
	Battlegroup     string      `json:"battlegroup"`
	Locale          string      `json:"locale"`
	Timezone        string      `json:"timezone"`
	ConnectedRealms []realmSlug `json:"connected_realms"`

	region region
}

func (rea realm) getAuctions(res resolver) (*auctions, error) {
	aucInfo, err := newAuctionInfoFromHTTP(rea, res)
	if err != nil {
		return nil, err
	}

	return aucInfo.getFirstAuctions(res)
}
