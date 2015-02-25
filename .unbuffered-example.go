package main

import (
	"fmt"
	"sync"
	"time"
)

/*
	jobHandler
*/
type jobHandler struct {
	waitGroup   *sync.WaitGroup
	workerCount int
	in          chan jobInterface
	out         chan jobInterface
}

func newJobHandler(workerCount int, out chan jobInterface) jobHandler {
	return jobHandler{
		waitGroup:   &sync.WaitGroup{},
		in:          make(chan jobInterface),
		out:         out,
		workerCount: workerCount,
	}
}

func (self jobHandler) Config() (*sync.WaitGroup, int, chan jobInterface, chan jobInterface) {
	return self.waitGroup, self.workerCount, self.in, self.out
}

func (self jobHandler) Process(job jobInterface) jobInterface { return job }

/*
	middleJobHandler
*/
type middleJobHandler struct {
	jobHandler
}

type middleJob struct {
	job
	name string
}

func newMiddleJobHandler(workerCount int, out chan jobInterface) middleJobHandler {
	return middleJobHandler{
		jobHandler: newJobHandler(workerCount, out),
	}
}

func (self middleJobHandler) Process(j jobInterface) jobInterface {
	v := middleJob{
		job: j.(job),
	}
	v.name = "lol"
	fmt.Println(fmt.Sprintf("middle working on %s", v.url))
	time.Sleep(time.Second * 5)
	v.middleFinishTime = time.Now()
	v.done = true
	return v
}

/*
	inJobHandler
*/
type inJobHandler struct {
	jobHandler
}

func newInJobHandler(workerCount int, out chan jobInterface) inJobHandler {
	return inJobHandler{
		jobHandler: newJobHandler(workerCount, out),
	}
}

func (self inJobHandler) Process(j jobInterface) jobInterface {
	v := j.(job)
	fmt.Println(fmt.Sprintf("in working on %s", v.url))
	time.Sleep(time.Second * 2)
	v.inFinishTime = time.Now()
	return v
}

/*
	jobHandlerInterface
*/
type jobHandlerInterface interface {
	Config() (*sync.WaitGroup, int, chan jobInterface, chan jobInterface)
	Process(jobInterface) jobInterface
}

func initializeJobHandler(jobHandler jobHandlerInterface) jobHandlerInterface {
	waitGroup, workerCount, in, out := jobHandler.Config()
	waitGroup.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer waitGroup.Done()
			for job := range in {
				job = jobHandler.Process(job)
				out <- job
			}
		}()
	}

	go func() {
		waitGroup.Wait()
		close(out)
	}()
	return jobHandler
}

/*
	jobInterface
*/
type jobInterface interface{}

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
	out := make(chan jobInterface)
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
	for outJob := range out {
		job := outJob.(middleJob)
		fmt.Println(fmt.Sprintf("job %s (%s) finished: %v", job.name, job.url, job.done))
		fmt.Println(fmt.Sprintf("%s is the start time", job.startTime.Format(WriteLayout)))
		fmt.Println(fmt.Sprintf("%s is the in finish time", job.inFinishTime.Format(WriteLayout)))
		fmt.Println(fmt.Sprintf("%s is the middle finish time", job.middleFinishTime.Format(WriteLayout)))
	}
	fmt.Println(fmt.Sprintf("Finished in %.2fs seconds", time.Since(startTime).Seconds()))
}
