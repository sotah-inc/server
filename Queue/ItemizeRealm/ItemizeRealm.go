package ItemizeRealm

import (
	"errors"
	"fmt"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Entity/Character"
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
	// misc
	job = newJob(inJob)
	realm := inJob.Realm

	if inJob.Err != nil {
		job.Err = errors.New(fmt.Sprintf("DownloadRealm.Job() had an error (%s)", inJob.Err.Error()))
		return
	}

	if !inJob.CanContinue() {
		return
	}

	/*
		character handling
	*/
	characterManager := Character.NewManager(realm, cacheClient)

	// gathering existing characters
	var (
		existingNames []string
		err           error
	)
	if existingNames, err = characterManager.GetNames(); err != nil {
		job.Err = errors.New(fmt.Sprintf("CharacterManager.GetNames() failed (%s)", err.Error()))
		return
	}

	// gathering new characters
	if err = characterManager.PersistAll(inJob.GetNewCharacters(existingNames)); err != nil {
		job.Err = errors.New(fmt.Sprintf("CharacterManager.PersistAll() failed (%s)", err.Error()))
		return
	}

	return
}

/*
	Job
*/
func newJob(inJob DownloadRealm.Job) Job {
	return Job{AuctionDataJob: inJob.AuctionDataJob}
}

type Job struct {
	Queue.AuctionDataJob
}
