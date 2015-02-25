package main

import (
	"fmt"
	"sync"
	"time"
)

/*
	jobHandler
*/
type jobHandler struct {
	waitGroup   *sync.WaitGroup
	workerCount int
	in          chan job
	out         chan job
}

func newJobHandler(workerCount int, out chan job) jobHandler {
	return jobHandler{
		waitGroup:   &sync.WaitGroup{},
		in:          make(chan job),
		out:         out,
		workerCount: workerCount,
	}
}

func (self jobHandler) Config() (*sync.WaitGroup, int, chan job, chan job) {
	return self.waitGroup, self.workerCount, self.in, self.out
}

func (self jobHandler) Process(job job) job { return job }

/*
	middleJobHandler
*/
type middleJobHandler struct {
	jobHandler
}

func newMiddleJobHandler(workerCount int, out chan job) middleJobHandler {
	return middleJobHandler{
		jobHandler: newJobHandler(workerCount, out),
	}
}

func (self middleJobHandler) Process(job job) job {
	fmt.Println(fmt.Sprintf("middle working on %s", job.url))
	time.Sleep(time.Second * 5)
	job.middleFinishTime = time.Now()
	job.done = true
	return job
}

/*
	inJobHandler
*/
type inJobHandler struct {
	jobHandler
}

func newInJobHandler(workerCount int, out chan job) inJobHandler {
	return inJobHandler{
		jobHandler: newJobHandler(workerCount, out),
	}
}

func (self inJobHandler) Process(job job) job {
	fmt.Println(fmt.Sprintf("in working on %s", job.url))
	time.Sleep(time.Second * 2)
	job.inFinishTime = time.Now()
	return job
}

/*
	jobHandlerInterface
*/
type jobHandlerInterface interface {
	Config() (*sync.WaitGroup, int, chan job, chan job)
	Process(job) job
}

func initializeJobHandler(jobHandler jobHandlerInterface) jobHandlerInterface {
	waitGroup, workerCount, in, out := jobHandler.Config()
	waitGroup.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer waitGroup.Done()
			for job := range in {
				job = jobHandler.Process(job)
				out <- job
			}
		}()
	}

	go func() {
		waitGroup.Wait()
		close(out)
	}()
	return jobHandler
}

/*
	job
*/
type job struct {
	url              string
	done             bool
	startTime        time.Time
	inFinishTime     time.Time
	middleFinishTime time.Time
}

/*
	main
*/
func main() {
	out := make(chan job)
	middleJobHandler := initializeJobHandler(newMiddleJobHandler(3, out)).(middleJobHandler)
	inJobHandler := initializeJobHandler(newInJobHandler(3, middleJobHandler.in)).(inJobHandler)

	// queueing up the in channel
	urls := []string{"http://google.ca/", "http://golang.org/", "http://youtube.com/"}
	go func() {
		for _, url := range urls {
			job := job{
				url:       url,
				startTime: time.Now(),
			}
			inJobHandler.in <- job
		}
		close(inJobHandler.in)
	}()

	// consuming the results
	startTime := time.Now()
	const WriteLayout = "2006-01-02 03:04:05PM"
	for job := range out {
		fmt.Println(fmt.Sprintf("job %s finished: %v", job.url, job.done))
		fmt.Println(fmt.Sprintf("%s is the start time", job.startTime.Format(WriteLayout)))
		fmt.Println(fmt.Sprintf("%s is the in finish time", job.inFinishTime.Format(WriteLayout)))
		fmt.Println(fmt.Sprintf("%s is the middle finish time", job.middleFinishTime.Format(WriteLayout)))
	}
	fmt.Println(fmt.Sprintf("Finished in %.2fs seconds", time.Since(startTime).Seconds()))
}
