package Queue

import (
	"sync"

	"github.com/ihsw/go-download/app/Entity"
)

/*
	funcs
*/
func Work(workerCount int, worker func(), postWork func()) {
	wg := &sync.WaitGroup{}
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			worker()
		}()
	}

	go func() {
		wg.Wait()
		postWork()
	}()
}

/*
	Jobs
*/
func NewJob() Job { return Job{} }

type Job struct {
	Err error
}

func NewRealmJob(realm Entity.Realm) RealmJob {
	return RealmJob{
		Job:   NewJob(),
		Realm: realm,
	}
}

type RealmJob struct {
	Job
	Realm Entity.Realm
}

func NewAuctionDataJob(realm Entity.Realm) AuctionDataJob {
	return AuctionDataJob{RealmJob: NewRealmJob(realm)}
}

type AuctionDataJob struct {
	RealmJob
	ResponseFailed bool
	AlreadyChecked bool
}

func (self AuctionDataJob) CanContinue() bool { return !(self.ResponseFailed || self.AlreadyChecked) }
