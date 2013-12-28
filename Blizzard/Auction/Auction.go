package Auction

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Util"
)

/*
	misc
*/
type Response struct {
	Files []File
}

type File struct {
	LastModified uint64
	Url          string
}

const URL_FORMAT = "http://%s/api/wow/auction/data/%s"

/*
	funcs
*/
func Get(realm Entity.Realm) (Response, error) {
	var (
		b        []byte
		err      error
		response Response
	)

	url := fmt.Sprintf(URL_FORMAT, realm.Region.Host, realm.Slug)
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
