package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type Job struct {
	err    error
	result string
}

func work(workerCount int, worker func() bool, postWork func()) {
	wg := &sync.WaitGroup{}
	alreadyDidPostWork := false
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			shouldCancel := worker()
			if !alreadyDidPostWork && shouldCancel {
				postWork()
				alreadyDidPostWork = true
			}
		}()
	}

	go func() {
		wg.Wait()
		if !alreadyDidPostWork {
			postWork()
		}
	}()
}

func DoWork(items []string, process func(string) (string, error)) chan Job {
	in := make(chan string)
	out := make(chan Job)

	worker := func() bool {
		for item := range in {
			job := Job{}
			if job.result, job.err = process(item); job.err != nil {
				out <- job
				return true
			}
			out <- job
		}
		return false
	}
	postWork := func() {
		close(out)
	}
	work(1, worker, postWork)

	go func() {
		for _, item := range items {
			in <- item
		}
		close(in)
	}()

	return out
}

func DoMoreWork(in chan Job, process func(Job) Job) (out chan Job, alternateOut chan Job) {
	out = make(chan Job)
	alternateOut = make(chan Job)
	putOut := func(job Job) {
		out <- job
		alternateOut <- job
	}

	worker := func() bool {
		for job := range in {
			if job = process(job); job.err != nil {
				putOut(job)
				return true
			}
			putOut(job)
		}
		return false
	}
	postWork := func() {
		close(out)
		close(alternateOut)
	}
	work(1, worker, postWork)

	return out, alternateOut
}

func main() {
	items := []string{"a", "b", "c", "d", "e"}
	out := DoWork(items, func(item string) (string, error) {
		fmt.Println(fmt.Sprintf("Working on %s", item))

		if item == "b" {
			return item, errors.New("found the letter b!")
		}

		time.Sleep(time.Second)

		return item, nil
	})
	moreOut, alternateMoreOut := DoMoreWork(out, func(job Job) Job {
		if job.err != nil {
			return job
		}

		fmt.Println(fmt.Sprintf("Doing more work on %s", job.result))
		time.Sleep(time.Second * 2)

		return job
	})

	// finishing up the alt channel
	alternateDone := make(chan struct{})
	go func() {
		for job := range alternateMoreOut {
			if err := job.err; err != nil {
				fmt.Println(fmt.Sprintf("alternateMoreOut: job %s had an error (%s)", job.result, err.Error()))
				continue
			}

			fmt.Println(fmt.Sprintf("alternateMoreOut: job %s was handled", job.result))
		}
		alternateDone <- struct{}{}
	}()

	// going over the more results
	for job := range moreOut {
		if err := job.err; err != nil {
			fmt.Println(fmt.Sprintf("moreOut: job %s had an error (%s)", job.result, err.Error()))
			continue
		}

		fmt.Println(fmt.Sprintf("moreOut: job %s was handled", job.result))
	}

	// waiting for alt to drain out
	<-alternateDone

	fmt.Println("done")
}
