package util

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

func Write(a interface{}) {
	fmt.Println(a)
}

func Download(url string) (interface{}, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	v := new(interface{})
	err = json.Unmarshal(b, v)
	return v, err
}
