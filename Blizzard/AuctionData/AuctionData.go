package AuctionData

import (
	"encoding/json"
	"fmt"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Util"
	"io/ioutil"
	"os"
	"time"
)

/*
	misc
*/
type ResponseWrapper struct {
	Response     Response `json:response`
	LastModified int64    `json:last-modified`
}

/*
	blizzard json response structs
*/
type Response struct {
	Realm    ResponseRealm `json:realm`
	Alliance Auctions      `json:alliance`
	Horde    Auctions      `json:horde`
	Neutral  Auctions      `json:neutral`
}

type ResponseRealm struct {
	Name string `json:name`
	Slug string `json:slug`
}

type Auctions struct {
	Auctions []Auction `json:auctions`
}

type Auction struct {
	Auc        uint64 `json:auc`
	Item       uint64 `json:item`
	Owner      string `json:owner`
	OwnerRealm string `json:ownerRealm`
	Bid        uint64 `json:bid`
	Buyout     uint64 `json:buyout`
	Quantity   uint64 `json:quantity`
	TimeLeft   string `json:timeLeft`
	Rand       int64  `json:rand`
	Seed       uint64 `json:seed`
}

/*
	funcs
*/
func Get(realm Entity.Realm, dataDirectory string, lastModified time.Time, url string) (Response, error) {
	/*
		misc
	*/
	var (
		response        Response
		responseWrapper ResponseWrapper
		err             error
		b               []byte
	)
	dataFilepath := fmt.Sprintf("%s/realm-%s.json.gz", dataDirectory, realm.Dump())

	/*
		checking locally
	*/
	// checking if the file exists
	var fileinfo os.FileInfo
	fileinfo, err = os.Stat(dataFilepath)
	if err != nil && !os.IsNotExist(err) {
		return response, err
	}

	/*
		attempting to load it
		if the wrapped last-modified == last-modified then return the wrapped response
	*/
	if fileinfo != nil {
		b, err = ioutil.ReadFile(dataFilepath)
		if err != nil {
			return response, err
		}

		b, err = Util.GzipDecode(b)
		if err != nil {
			return response, err
		}

		err = json.Unmarshal(b, &responseWrapper)
		if err != nil {
			return response, err
		}

		wrappedLastModified := time.Unix(responseWrapper.LastModified, 0)
		if wrappedLastModified == lastModified {
			return responseWrapper.Response, nil
		}
		err = os.Remove(dataFilepath)
		if err != nil {
			return response, err
		}
	}

	/*
		falling back to checking the api
	*/
	// downloading from the api
	b, err = Util.Download(url)
	if err != nil {
		return response, err
	}

	// attempting to json-decode it
	err = json.Unmarshal(b, &response)
	if err != nil {
		return response, err
	}

	// re-encoding it with the last-modified and dumping it back to disk
	responseWrapper = ResponseWrapper{
		Response:     response,
		LastModified: lastModified.Unix(),
	}
	b, err = json.Marshal(responseWrapper)
	if err != nil {
		return response, err
	}
	b, err = Util.GzipEncode(b)
	if err != nil {
		return response, err
	}
	err = ioutil.WriteFile(dataFilepath, b, 0755)
	if err != nil {
		return response, err
	}

	return response, nil
}
