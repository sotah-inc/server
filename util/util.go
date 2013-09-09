package util

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

func Write(a interface{}) {
	fmt.Println(a)
}

func Download(url string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(b), nil
}
