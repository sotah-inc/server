package blizzard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sotah-inc/server/app/pkg/util"
)

const auctionInfoURLFormat = "https://%s/wow/auction/data/%s"

// DefaultGetAuctionInfoURL generates a url for fetching auction-info
func DefaultGetAuctionInfoURL(regionHostname string, realmSlug RealmSlug) string {
	return fmt.Sprintf(auctionInfoURLFormat, regionHostname, realmSlug)
}

// GetAuctionInfoURLFunc defines the expected function signature for generating an auction-info url
type GetAuctionInfoURLFunc func(string, RealmSlug) string

// NewAuctionInfoFromHTTP downloads json from the api
func NewAuctionInfoFromHTTP(uri string) (AuctionInfo, ResponseMeta, error) {
	resp, err := Download(uri)
	if err != nil {
		return AuctionInfo{}, ResponseMeta{}, err
	}

	if resp.Status != 200 {
		return AuctionInfo{}, ResponseMeta{}, errors.New("Status was not 200")
	}

	aInfo, err := NewAuctionInfo(resp.Body)
	if err != nil {
		return AuctionInfo{}, ResponseMeta{}, err
	}

	return aInfo, resp, nil
}

// NewAuctionInfoFromFilepath parses a json file
func NewAuctionInfoFromFilepath(relativeFilepath string) (AuctionInfo, error) {
	body, err := util.ReadFile(relativeFilepath)
	if err != nil {
		return AuctionInfo{}, err
	}

	return NewAuctionInfo(body)
}

// NewAuctionInfo parses a json byte array
func NewAuctionInfo(body []byte) (AuctionInfo, error) {
	a := &AuctionInfo{}
	if err := json.Unmarshal(body, a); err != nil {
		return AuctionInfo{}, err
	}

	return *a, nil
}

// AuctionInfo describes the auction-info returned from the api
type AuctionInfo struct {
	Files []AuctionFile `json:"files"`
}

// GetFirstAuctions returns the auctions from the first item in the files listing
func (aInfo AuctionInfo) GetFirstAuctions() (Auctions, error) {
	if len(aInfo.Files) == 0 {
		return Auctions{}, errors.New("Cannot fetch first auctions with blank files")
	}

	return aInfo.Files[0].GetAuctions()
}

// AuctionFile points to the url for fetching auctions
type AuctionFile struct {
	URL          string `json:"url"`
	LastModified int64  `json:"lastModified"`
}

// GetAuctions returns the auctions from a given file
func (aFile AuctionFile) GetAuctions() (Auctions, error) {
	return NewAuctionsFromHTTP(aFile.URL)
}

// LastModifiedAsTime returns a parsed last-modified
func (aFile AuctionFile) LastModifiedAsTime() time.Time {
	return time.Unix(aFile.LastModified/1000, 0)
}

// DefaultGetAuctionsURL defines the default format of a provided url for downloading auctions
func DefaultGetAuctionsURL(url string) string { return url }

// GetAuctionsURLFunc defines the expected function signature when generating a url for downloading auctions
type GetAuctionsURLFunc func(url string) string

// NewAuctionsFromGcloudObject fetches json from a gcloud store object
func NewAuctionsFromGcloudObject(ctx context.Context, obj *storage.ObjectHandle) (Auctions, error) {
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return Auctions{}, err
	}
	defer reader.Close()

	body, err := ioutil.ReadAll(reader)
	if err != nil {
		return Auctions{}, err
	}

	return NewAuctions(body)
}

// NewAuctionsFromHTTP fetches json from the http api for auctions
func NewAuctionsFromHTTP(url string) (Auctions, error) {
	resp, err := Download(url)
	if err != nil {
		return Auctions{}, err
	}

	return NewAuctions(resp.Body)
}

// NewAuctionsFromFilepath parses a json file for auctions
func NewAuctionsFromFilepath(relativeFilepath string) (Auctions, error) {
	body, err := util.ReadFile(relativeFilepath)
	if err != nil {
		return Auctions{}, err
	}

	return NewAuctions(body)
}

// NewAuctionsFromGzFilepath parsed a gzipped json file for auctions
func NewAuctionsFromGzFilepath(relativeFilepath string) (Auctions, error) {
	body, err := util.ReadFile(relativeFilepath)
	if err != nil {
		return Auctions{}, err
	}

	decodedBody, err := util.GzipDecode(body)
	if err != nil {
		return Auctions{}, err
	}

	return NewAuctions(decodedBody)
}

// NewAuctions parses a byte array for auctions
func NewAuctions(body []byte) (Auctions, error) {
	a := &Auctions{}
	if err := json.Unmarshal(body, a); err != nil {
		return Auctions{}, err
	}

	return *a, nil
}

// Auctions describes the auctions returned from the api
type Auctions struct {
	Realms   []AuctionRealm `json:"realms"`
	Auctions []Auction      `json:"auctions"`
}

// OwnerNames returns all owners in this auctions dump
func (aucs Auctions) OwnerNames() []string {
	result := map[string]struct{}{}
	for _, auc := range aucs.Auctions {
		result[auc.Owner] = struct{}{}
	}

	out := []string{}
	for v := range result {
		out = append(out, v)
	}

	return out
}

// AuctionRealm is the realm associated with an auctions response
type AuctionRealm struct {
	Name string    `json:"name"`
	Slug RealmSlug `json:"slug"`
}

// Auction describes a single auction
type Auction struct {
	Auc        int64  `json:"auc"`
	Item       ItemID `json:"item"`
	Owner      string `json:"owner"`
	OwnerRealm string `json:"ownerRealm"`
	Bid        int64  `json:"bid"`
	Buyout     int64  `json:"buyout"`
	Quantity   int64  `json:"quantity"`
	TimeLeft   string `json:"timeLeft"`
	Rand       int64  `json:"rand"`
	Seed       int64  `json:"seed"`
	Context    int64  `json:"context"`
}
