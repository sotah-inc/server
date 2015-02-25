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
	return middleJobHandler{
		jobHandler: newJobHandler(workerCount, out),
	}
}

func (self middleJobHandler) process(job job) job {
	fmt.Println(fmt.Sprintf("middle working on %s", job.url))
	time.Sleep(time.Second * 5)
	job.middleFinishTime = time.Now()
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

func (self inJobHandler) process(job job) job {
	fmt.Println(fmt.Sprintf("in working on %s", job.url))
	time.Sleep(time.Second * 2)
	job.inFinishTime = time.Now()
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
	middleJobHandler := newMiddleJobHandler(1, outJobHandler.in)
	inJobHandler := newInJobHandler(4, middleJobHandler.in)

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
		fmt.Println(fmt.Sprintf("%s is the start time", job.startTime.Format(WriteLayout)))
		fmt.Println(fmt.Sprintf("%s is the in finish time", job.inFinishTime.Format(WriteLayout)))
		fmt.Println(fmt.Sprintf("%s is the middle finish time", job.middleFinishTime.Format(WriteLayout)))
	}
}
