package main

import (
	"fmt"
	"sync"
	"time"
)

type job struct {
	url              string
	done             bool
	startTime        time.Time
	inFinishTime     time.Time
	middleFinishTime time.Time
}

func processIn(in chan job, out chan job) {
	for job := range in {
		fmt.Println(fmt.Sprintf("processIn working on %s", job.url))

		time.Sleep(time.Second * 2)
		job.inFinishTime = time.Now()

		out <- job
	}
}

func processMiddle(in chan job, out chan job) {
	for job := range in {
		fmt.Println(fmt.Sprintf("processMiddle working on %s", job.url))

		time.Sleep(time.Second * 1)
		job.middleFinishTime = time.Now()
		job.done = true

		out <- job
	}
}

func main() {
	in := make(chan job)
	middle := make(chan job)
	out := make(chan job)

	inWg := &sync.WaitGroup{}
	const inWorkerCount = 4
	inWg.Add(inWorkerCount)
	for i := 0; i < inWorkerCount; i++ {
		go func() {
			defer inWg.Done()
			processIn(in, middle)
		}()
	}

	middleWg := &sync.WaitGroup{}
	const middleWorkerCount = 1
	middleWg.Add(middleWorkerCount)
	for i := 0; i < middleWorkerCount; i++ {
		go func() {
			defer middleWg.Done()
			processMiddle(middle, out)
		}()
	}

	// queueing up the in channel
	urls := []string{"http://google.ca/", "http://golang.org/", "http://youtube.com/"}
	go func() {
		for _, url := range urls {
			job := job{
				url:       url,
				startTime: time.Now(),
			}
			in <- job
		}
		close(in)
	}()

	go func() {
		inWg.Wait()
		close(middle)
	}()
	go func() {
		middleWg.Wait()
		close(out)
	}()

	// consuming the results
	for job := range out {
		fmt.Println(fmt.Sprintf("job %s finished: %v", job.url, job.done))
	}
}
