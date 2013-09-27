package Auction

import (
	"encoding/json"
	"fmt"
	"github.com/ihsw/go-download/Util"
)

type Auction struct {
	Files []File
}

type File struct {
	LastModified uint64
	Url          string
}

const URL_FORMAT = "http://%s/api/wow/auction/data/%s"

func Get(host string, realm string) (Auction, error) {
	var auction Auction

	b, err := Util.Download(fmt.Sprintf(URL_FORMAT, host, realm))
	if err != nil {
		return auction, err
	}

	err = json.Unmarshal(b, &auction)
	if err != nil {
		return auction, err
	}

	return auction, nil
}
