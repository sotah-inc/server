package main

import (
	"fmt"
	"sync"
	"time"
)

type job struct {
	url  string
	done bool
}

func process(in chan job, middle chan job, inWg *sync.WaitGroup) {
	defer inWg.Done()
	for job := range in {
		fmt.Println(fmt.Sprintf("working on %s", job.url))
		time.Sleep(time.Second * 2)
		job.done = true
		middle <- job
	}
}

func main() {
	// misc
	inWg := new(sync.WaitGroup)
	in := make(chan job)
	middle := make(chan job)
	out := make(chan job)

	// spawning some workers
	const inWorkerCount = 4
	inWg.Add(inWorkerCount)
	for i := 0; i < inWorkerCount; i++ {
		go process(in, middle, inWg)
	}

	// queueing up the in channel
	urls := []string{"http://google.ca/", "http://golang.org/", "http://youtube.com/"}
	go func() {
		for _, url := range urls {
			in <- job{url: url}
		}
		close(in)
	}()

	// waiting for the in to drain
	go func() {
		inWg.Wait()
		close(middle)
	}()

	// queueing up the middle channel
	middleWg := new(sync.WaitGroup)
	inWg.Add(1)
	go func() {
		for {
		}
	}()

	// consuming the results
	jobs := []job{}
	for job := range middle {
		fmt.Println(fmt.Sprintf("job %s finished: %v", job.url, job.done))
		jobs = append(jobs, job)
	}

	fmt.Println(fmt.Sprintf("done %d jobs", len(jobs)))
}
