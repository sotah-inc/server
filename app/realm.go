package app

import "github.com/ihsw/go-download/app/util"
import "fmt"

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
		fmt.Println("Spinning up a worker")
		for rea := range in {
			fmt.Printf("Fetching auctions for %s-%s\n", rea.region.Hostname, rea.Slug)
			aucs, err := rea.getAuctions(res)
			out <- getAuctionsJob{err: err, realm: rea, auctions: aucs}
		}
	}
	postWork := func() {
		fmt.Println("Finished pushing out, closing")
		close(out)
	}
	util.Work(4, worker, postWork)

	// queueing up the realms
	go func() {
		fmt.Printf("Ingesting %d realms\n", len(reas))
		for _, rea := range reas {
			in <- rea
		}

		fmt.Println("Finished ingesting, closing")
		close(in)
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
