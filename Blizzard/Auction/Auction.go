package Auction

import (
	"encoding/json"
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
	Length   int64
	Realm    Entity.Realm
	Error    error
}

/*
	funcs
*/
func Get(realm Entity.Realm, c chan Result) {
	var (
		b        []byte
		err      error
		response Response
	)
	result := Result{
		Response: response,
		Realm:    realm,
		Length:   0,
		Error:    nil,
	}

	fmt.Println(fmt.Sprintf("Downloading %s-%s...", realm.Region.Name, realm.Slug))
	url := fmt.Sprintf(URL_FORMAT, realm.Region.Host, realm.Slug)
	b, result.Error = Util.Download(url)
	if err = result.Error; err != nil {
		c <- result
		return
	}

	result.Error = json.Unmarshal(b, &response)
	if err = result.Error; err != nil {
		c <- result
	}

	result.Response = response
	result.Length = int64(len(b))
	c <- result
}
