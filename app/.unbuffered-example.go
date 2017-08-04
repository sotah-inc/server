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

func work(workerCount int, worker func(), postWork func()) {
	wg := &sync.WaitGroup{}
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			worker()
		}()
	}

	go func() {
		wg.Wait()
		postWork()
	}()
}

func DoWork(items []string, process func(string) Job) chan Job {
	in := make(chan string)
	out := make(chan Job)

	worker := func() {
		for item := range in {
			out <- process(item)
		}
	}
	postWork := func() {
		close(out)
	}
	work(4, worker, postWork)

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

	worker := func() {
		for job := range in {
			job = process(job)
			out <- job
			alternateOut <- job
		}
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
	out := DoWork(items, func(item string) (job Job) {
		job.result = item
		if item == "b" {
			job.err = errors.New("found the letter b!")
		}
		return
	})
	moreOut, alternateMoreOut := DoMoreWork(out, func(item Job) (job Job) {
		job = item
		if job.err != nil {
			return job
		}

		time.Sleep(time.Second * 2)

		return job
	})

	// finishing up the alt channel
	alternateDone := make(chan struct{})
	go func() {
		stuff := []string{}
		for job := range alternateMoreOut {
			if err := job.err; err != nil {
				fmt.Println(fmt.Sprintf("alternateMoreOut: job %s had an error (%s)", job.result, err.Error()))
				continue
			}

			stuff = append(stuff, job.result)
		}

		fmt.Println(fmt.Sprintf("alternateMoreOut stuff: %s", stuff))

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
