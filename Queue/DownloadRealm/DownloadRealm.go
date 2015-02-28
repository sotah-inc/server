package DownloadRealm

import (
	"github.com/ihsw/go-download/Blizzard/AuctionData"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Queue"
)

func NewJob(realm Entity.Realm) Job {
	return Job{AuctionDataJob: Queue.NewAuctionDataJob(realm)}
}

type Job struct {
	Queue.AuctionDataJob
	AuctionDataResponse *AuctionData.Response
}

func DoWork(in chan Entity.Realm, process func(Entity.Realm) Job) chan Job {
	out := make(chan Job)

	worker := func() {
		for realm := range in {
			out <- process(realm)
		}
	}
	postWork := func() { close(out) }
	Queue.Work(4, worker, postWork)

	return out
}
