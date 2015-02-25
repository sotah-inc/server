package main

import (
	"fmt"
	"sync"
	"time"
)

/*
	middleJobHandler
*/
type middleJobHandler struct {
	jobHandler
}

func newMiddleJobHandler(workerCount int, out chan job) middleJobHandler {
	jobHandler := middleJobHandler{
		jobHandler: newJobHandler(workerCount, out),
	}
	return jobHandler
}

func (self middleJobHandler) process(job job) job {
	fmt.Println(fmt.Sprintf("middle working on %s", job.url))
	time.Sleep(time.Second * 5)
	job.middleFinishTime = time.Now()
	return job
}

/*
	jobHandler
*/
type jobHandler struct {
	waitGroup *sync.WaitGroup
	in        chan job
	out       chan job
}

func newJobHandler(workerCount int, out chan job) jobHandler {
	jobHandler := jobHandler{
		waitGroup: &sync.WaitGroup{},
		in:        make(chan job),
		out:       out,
	}

	jobHandler.waitGroup.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer jobHandler.waitGroup.Done()
			for job := range jobHandler.in {
				job = jobHandler.process(job)
				jobHandler.out <- job
			}
		}()
	}

	go func() {
		jobHandler.waitGroup.Wait()
		close(jobHandler.out)
	}()

	return jobHandler
}

func (self jobHandler) process(job job) job {
	fmt.Println(fmt.Sprintf("working on %s", job.url))
	time.Sleep(time.Second * 2)
	job.inFinishTime = time.Now()
	return job
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
	outJobHandler := newJobHandler(1, out)
	middleJobHandler := newJobHandler(1, outJobHandler.in)
	inJobHandler := newJobHandler(4, middleJobHandler.in)

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
	const WriteLayout = "2006-01-02 03:04:05PM"
	for job := range out {
		fmt.Println(fmt.Sprintf("job %s finished: %v", job.url, job.done))
		fmt.Println(fmt.Sprintf("job %s start time: %s", job.url, job.startTime.Format(WriteLayout)))
		fmt.Println(fmt.Sprintf("job %s in finish time: %s", job.url, job.inFinishTime.Format(WriteLayout)))
		fmt.Println(fmt.Sprintf("job %s middle finish time: %s", job.url, job.middleFinishTime.Format(WriteLayout)))
	}
}
