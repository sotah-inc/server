package main

import (
	"fmt"
	"sync"
	"time"
)

type jobHandler struct {
	waitGroup   *sync.WaitGroup
	in          chan job
	out         chan job
	workerCount int
}

func (self jobHandler) WaitGroup() *sync.WaitGroup { return self.waitGroup }
func (self jobHandler) In() chan job               { return self.in }
func (self jobHandler) Out() chan job              { return self.out }
func (self jobHandler) WorkerCount() int           { return self.workerCount }
func (self jobHandler) Process(job job) job        { return job }

func newJobHandler(workerCount int, out chan job) jobHandler {
	return jobHandler{
		waitGroup:   &sync.WaitGroup{},
		in:          make(chan job),
		out:         out,
		workerCount: workerCount,
	}
}

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

type jobHandlerInterface interface {
	WaitGroup() *sync.WaitGroup
	In() chan job
	Out() chan job
	Process(job) job
	WorkerCount() int
}

func initializeJobHandler(jobHandler jobHandlerInterface) jobHandlerInterface {
	jobHandler.WaitGroup().Add(jobHandler.WorkerCount())
	for i := 0; i < jobHandler.WorkerCount(); i++ {
		go func() {
			defer jobHandler.WaitGroup().Done()
			for job := range jobHandler.In() {
				job = jobHandler.Process(job)
				jobHandler.Out() <- job
			}
		}()
	}

	go func() {
		jobHandler.WaitGroup().Wait()
		close(jobHandler.Out())
	}()
	return jobHandler
}

type job struct {
	url              string
	done             bool
	startTime        time.Time
	inFinishTime     time.Time
	middleFinishTime time.Time
}

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
