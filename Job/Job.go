package Job

import (
	"sync"
)

type Job struct {
	Err error
}

func DoWork(items []interface{}, process func(interface{}) Job) chan Job {
	wg := &sync.WaitGroup{}
	in := make(chan interface{})
	out := make(chan Job)
	workerCount := 4

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
		wg.Wait()
		close(out)
	}()

	go func() {
		for _, item := range items {
			in <- item
		}
		close(in)
	}()

	return out
}
