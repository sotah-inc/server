package ItemizeRealm

import (
	"errors"
	"fmt"

	"github.com/ihsw/go-download/app/Cache"
	"github.com/ihsw/go-download/app/Entity"
	"github.com/ihsw/go-download/app/Entity/Character"
	"github.com/ihsw/go-download/app/Queue"
	"github.com/ihsw/go-download/app/Queue/DownloadRealm"
)

/*
	funcs
*/
func DoWork(in chan DownloadRealm.Job, cacheClient Cache.Client) (chan Job, chan error) {
	out := make(chan Job)
	alternateOut := make(chan Job)
	alternateOutDone := make(chan error)

	worker := func() {
		for inJob := range in {
			job := process(inJob, cacheClient)
			out <- job
			alternateOut <- job
		}
	}
	postWork := func() {
		close(out)
		close(alternateOut)
	}
	Queue.Work(1, worker, postWork)

	// gathering up the list of unique items
	go func() {
		// waiting for the jobs to drain out
		jobs := Jobs{}
		for job := range alternateOut {
			if !job.CanContinue() {
				continue
			}

			jobs.list = append(jobs.list, job)
		}

		// gathering existing items
		var (
			existingBlizzIds []int64
			err              error
		)
		itemManager := Entity.ItemManager{Client: cacheClient}
		if existingBlizzIds, err = itemManager.GetBlizzIds(); err != nil {
			alternateOutDone <- err
		}
		if err = itemManager.PersistAll(jobs.getNewItems(existingBlizzIds)); err != nil {
			alternateOutDone <- err
		}

		alternateOutDone <- nil
	}()

	return out, alternateOutDone
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

	/*
		item handling
	*/
	job.blizzItemIds = inJob.GetBlizzItemIds()

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
	blizzItemIds []int64
}

/*
	Jobs
*/
type Jobs struct {
	list []Job
}

func (self Jobs) getNewItems(existingBlizzIds []int64) (newItems []Entity.Item) {
	// gathering the blizz-ids for uniqueness
	existingBlizzIdFlags := map[int64]struct{}{}
	for _, blizzId := range existingBlizzIds {
		existingBlizzIdFlags[blizzId] = struct{}{}
	}

	newBlizzIds := map[int64]struct{}{}
	for _, result := range self.list {
		for _, blizzId := range result.blizzItemIds {
			_, ok := existingBlizzIdFlags[blizzId]
			if ok {
				continue
			}

			newBlizzIds[blizzId] = struct{}{}
		}
	}

	newItems = []Entity.Item{}
	i := 0
	for blizzId, _ := range newBlizzIds {
		newItems = append(newItems, Entity.Item{BlizzId: blizzId})
		i++
	}

	return newItems
}
