package util

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

const writeLayout = "2006-01-02 3:04:04PM"

func Write(s string) {
	fmt.Println(fmt.Sprintf("[%s] %s", time.Now().Format(writeLayout), s))
}

func Download(url string) (map[string]interface{}, error) {
	v := map[string]interface{}{}

	resp, err := http.Get(url)
	if err != nil {
		return v, err
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return v, err
	}

	err = json.Unmarshal(b, &v)
	return v, err
}
