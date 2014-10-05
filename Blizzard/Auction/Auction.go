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
func Get(realm Entity.Realm, apiKey string) (response Response, err error) {
	var b []byte
	url := fmt.Sprintf(URL_FORMAT, realm.Region.Host, realm.Slug, apiKey)
	b, err = Util.Download(url)
	if err != nil {
		return response, err
	}

	err = json.Unmarshal(b, &response)
	if err != nil {
		return response, err
	}

	if len(response.Files) == 0 {
		return response, errors.New("Response.Files length was zero")
	} else if len(response.Files) > 1 {
		return response, errors.New("Response.Files length was >1")
	}

	return response, nil
}
