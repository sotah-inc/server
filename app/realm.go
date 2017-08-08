package app

import "github.com/ihsw/go-download/app/util"

type getAuctionsJob struct {
	err      error
	realm    realm
	auctions *auctions
}

type realms []realm

func (reas realms) getAuctions(res resolver) chan getAuctionsJob {
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
	postWork := func() { close(out) }
	util.Work(4, worker, postWork)

	// queueing up the realms
	go func() {
		for _, rea := range reas {
			in <- rea
		}
	}()

	return out
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
	aucInfo, err := newAuctionInfo(rea, res)
	if err != nil {
		return nil, err
	}

	return aucInfo.getFirstAuctions(res)
}
