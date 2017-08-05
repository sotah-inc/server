package Auction

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ihsw/go-download/app/Entity"
	"github.com/ihsw/go-download/app/Util"
)

/*
	blizzard json response structs
*/
type Response struct {
	Files []File
}

type File struct {
	LastModified int64
	Url          string
}

const URL_FORMAT = "https://%s/wow/auction/data/%s?apikey=%s"

/*
	funcs
*/
func Get(realm Entity.Realm, apiKey string) (response *Response, err error) {
	url := fmt.Sprintf(URL_FORMAT, realm.Region.Host, realm.Slug, apiKey)
	var b []byte
	if b, err = Util.Download(url); err != nil {
		err = errors.New(fmt.Sprintf("Util.Download() for %s failed (%s)", url, err.Error()))
		return
	}

	if err = json.Unmarshal(b, &response); err != nil {
		return nil, nil
	}

	if len(response.Files) == 0 {
		return nil, nil
	}

	return response, nil
}
