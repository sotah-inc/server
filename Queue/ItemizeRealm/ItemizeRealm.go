package ItemizeRealm

import (
	"fmt"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Queue"
	"github.com/ihsw/go-download/Queue/DownloadRealm"
)

/*
	funcs
*/
func DoWork(in chan DownloadRealm.Job, cacheClient Cache.Client) chan Job {
	out := make(chan Job)

	worker := func() {
		for inJob := range in {
			out <- process(inJob, cacheClient)
		}
	}
	postWork := func() { close(out) }
	Queue.Work(4, worker, postWork)

	return out
}

func process(inJob DownloadRealm.Job, cacheClient Cache.Client) (job Job) {
	fmt.Println(fmt.Sprintf("Working on %s", inJob.Realm.Dump()))

	return newJob(inJob)
}

/*
	Job
*/
func newJob(inJob DownloadRealm.Job) Job {
	return Job{AuctionDataJob: Queue.NewAuctionDataJob(inJob.Realm)}
}

type Job struct {
	Queue.AuctionDataJob
}
