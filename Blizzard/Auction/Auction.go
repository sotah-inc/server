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
	chan structs
*/
type Result struct {
	Response Response
	Realm    Entity.Realm
	Error    error
}

/*
	funcs
*/
func Get(realm Entity.Realm) Result {
	var (
		b        []byte
		response Response
	)
	result := Result{
		Response: response,
		Realm:    realm,
		Error:    nil,
	}

	url := fmt.Sprintf(URL_FORMAT, realm.Region.Host, realm.Slug)
	b, result.Error = Util.Download(url)
	if result.Error != nil {
		return result
	}

	result.Error = json.Unmarshal(b, &response)
	if result.Error != nil {
		return result
	}

	if len(response.Files) == 0 {
		result.Error = errors.New("Response.Files length was zero")
		return result
	} else if len(response.Files) > 1 {
		result.Error = errors.New("Response.Files length was >1")
		return result
	}

	result.Response = response
	return result
}
