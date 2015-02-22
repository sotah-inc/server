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

func process(in chan job, out chan job, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range in {
		fmt.Println(fmt.Sprintf("working on %s", job.url))
		time.Sleep(time.Second * 2)
		job.done = true
		out <- job
	}
}

func main() {
	// misc
	wg := new(sync.WaitGroup)
	in := make(chan job)
	out := make(chan job)
	const workerCount = 4

	// spawning some workers
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go process(in, out, wg)
	}

	// queueing up the in channel
	urls := []string{"http://google.ca/", "http://golang.org/", "http://youtube.com/"}
	go func() {
		for _, url := range urls {
			in <- job{url: url}
		}
		close(in)
	}()

	// waiting for results to drain out
	go func() {
		wg.Wait()
		close(out)
	}()

	// consuming the results
	jobs := []job{}
	for job := range out {
		fmt.Println(fmt.Sprintf("job %s finished: %v", job.url, job.done))
		jobs = append(jobs, job)
	}

	fmt.Println(fmt.Sprintf("done %d jobs", len(jobs)))
}
