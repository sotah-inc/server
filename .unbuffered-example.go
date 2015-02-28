package main

import (
	"fmt"
	"sync"
	"time"
)

type Job struct {
	err    error
	result string
}

func DoWork(items []string, process func(string) Job) chan Job {
	in := make(chan string)
	out := make(chan Job)

	const workerCount = 4
	wg := &sync.WaitGroup{}
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			for item := range in {
				out <- process(item)
			}
		}()
	}

	go func() {
		for _, item := range items {
			in <- item
		}
		close(in)
		wg.Wait()
		close(out)
	}()

	return out
}

func DoMoreWork(in chan Job, process func(Job) Job) (chan Job, chan Job) {
	out := make(chan Job)
	alternateOut := make(chan Job)

	const workerCount = 4
	wg := &sync.WaitGroup{}
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			for job := range in {
				job := process(job)
				out <- job
				alternateOut <- job
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
		close(alternateOut)
	}()

	return out, alternateOut
}

func main() {
	items := []string{"a", "b", "c", "d", "e"}
	out := DoWork(items, func(item string) Job {
		fmt.Println(fmt.Sprintf("Working on %s", item))
		time.Sleep(time.Second)
		return Job{result: item}
	})
	moreOut, alternateMoreOut := DoMoreWork(out, func(job Job) Job {
		fmt.Println(fmt.Sprintf("Doing more work on %s", job.result))
		time.Sleep(time.Second * 2)
		return job
	})

	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		for job := range moreOut {
			if err := job.err; err != nil {
				fmt.Println(fmt.Sprintf("job %s had an error: %s", job.result, err.Error()))
				continue
			}
			fmt.Println(fmt.Sprintf("Job finished: %s", job.result))
		}
	}()
	go func() {
		defer wg.Done()
		for job := range alternateMoreOut {
			if err := job.err; err != nil {
				fmt.Println(fmt.Sprintf("job %s had an error: %s", job.result, err.Error()))
				continue
			}
			fmt.Println(fmt.Sprintf("Doing alternate handling of %s", job.result))
		}
	}()
	wg.Wait()

	fmt.Println("done")
}
