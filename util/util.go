package util

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"runtime"
	"time"
)

const writeLayout = "2006-01-02 3:04:04PM"

func MemoryUsage() uint64 {
	s := &runtime.MemStats{}
	runtime.ReadMemStats(s)
	return s.Alloc
}

func Write(s string) {
	fmt.Println(fmt.Sprintf("[%s] %s", time.Now().Format(writeLayout), s))
}

func Conclude() {
	Write(fmt.Sprintf("Success! %.2f MB", float64(MemoryUsage())/1000/1000))
}

func Download(url string) ([]byte, error) {
	var v []byte

	resp, err := http.Get(url)
	if err != nil {
		return v, err
	}
	defer resp.Body.Close()

	return ioutil.ReadAll(resp.Body)
}
