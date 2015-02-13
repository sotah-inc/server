package Auction

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Util"
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
	b, err = Util.Download(url)
	if err != nil {
		err = errors.New(fmt.Sprintf("Util.Download() for %s failed (%s)", url, err.Error()))
		return
	}

	err = json.Unmarshal(b, &response)
	if err != nil {
		return nil, nil
	}

	if len(response.Files) == 0 {
		err = errors.New(fmt.Sprintf("Response.Files length was zero for %s", url))
		return
	} else if len(response.Files) > 1 {
		err = errors.New(fmt.Sprintf("Response.Files length was >1 for %s", url))
		return
	}

	return response, nil
}
