package DownloadRealm

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ihsw/go-download/Blizzard/Auction"
	"github.com/ihsw/go-download/Blizzard/AuctionData"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Entity/Character"
	"github.com/ihsw/go-download/Queue"
	"github.com/ihsw/go-download/Util"
	"io/ioutil"
	"os"
	"time"
)

/*
	funcs
*/
func DoWork(in chan Entity.Realm, cacheClient Cache.Client) chan Job {
	out := make(chan Job)

	worker := func() {
		for realm := range in {
			out <- process(realm, cacheClient)
		}
	}
	postWork := func() { close(out) }
	Queue.Work(4, worker, postWork)

	return out
}

func process(realm Entity.Realm, cacheClient Cache.Client) (job Job) {
	// misc
	realmManager := Entity.NewRealmManager(realm.Region, cacheClient)
	job = newJob(realm)

	// fetching the auction info
	var (
		auctionResponse *Auction.Response
		err             error
	)
	if auctionResponse, err = Auction.Get(realm, cacheClient.ApiKey); err != nil {
		job.Err = errors.New(fmt.Sprintf("Auction.Get() failed (%s)", err.Error()))
		return
	}

	// optionally halting on empty response
	if auctionResponse == nil {
		job.ResponseFailed = true
		return
	}

	file := auctionResponse.Files[0]

	// checking whether the file has already been downloaded
	lastModified := time.Unix(file.LastModified/1000, 0)
	if !realm.LastDownloaded.IsZero() && (realm.LastDownloaded.Equal(lastModified) || realm.LastDownloaded.After(lastModified)) {
		job.AlreadyChecked = true
		return
	}

	// fetching the actual auction data
	if job.AuctionDataResponse = AuctionData.Get(realm, file.Url); job.AuctionDataResponse == nil {
		job.ResponseFailed = true
		return
	}

	// dumping the auction data for parsing after itemize-results are tabulated
	if err = job.dumpData(); err != nil {
		job.Err = errors.New(fmt.Sprintf("DownloadRealm.Job.dumpData() failed (%s)", err.Error()))
		return
	}

	// flagging the realm as having been downloaded
	realm.LastDownloaded = lastModified
	realm.LastChecked = time.Now()
	if realm, err = realmManager.Persist(realm); err != nil {
		job.Err = errors.New(fmt.Sprintf("RealmManager.Persist() failed (%s)", err.Error()))
		return
	}

	return
}

/*
	Job
*/
func newJob(realm Entity.Realm) Job {
	return Job{AuctionDataJob: Queue.NewAuctionDataJob(realm)}
}

type Job struct {
	Queue.AuctionDataJob
	AuctionDataResponse *AuctionData.Response
}

func (self Job) GetNewCharacters(existingNames []string) (newCharacters []Character.Character) {
	// misc
	auctions := self.AuctionDataResponse.Auctions.Auctions

	// gathering the names for uniqueness
	existingNameFlags := make(map[string]struct{})
	for _, name := range existingNames {
		existingNameFlags[name] = struct{}{}
	}

	newNames := make(map[string]struct{})
	for _, auction := range auctions {
		name := auction.Owner
		_, ok := existingNameFlags[name]
		if ok {
			continue
		}

		newNames[name] = struct{}{}
	}

	// doing a second pass to fill new ones in
	newCharacters = make([]Character.Character, len(newNames))
	i := 0
	for name, _ := range newNames {
		newCharacters[i] = Character.Character{
			Name:  name,
			Realm: self.Realm,
		}
		i++
	}

	return newCharacters
}

func (self Job) dumpData() (err error) {
	realm := self.Realm

	var wd string
	wd, err = os.Getwd()
	if err != nil {
		return
	}

	folder := fmt.Sprintf("%s/json/%s", wd, realm.Region.Name)
	if _, err = os.Stat(folder); err != nil && os.IsNotExist(err) {
		if err = os.MkdirAll(folder, 0777); err != nil {
			return
		}
	}

	// removing the old non-gzipped data
	oldDest := fmt.Sprintf("%s/%s.json", folder, realm.Slug)
	if _, err = os.Stat(oldDest); err == nil {
		err = os.Remove(oldDest)
		if err != nil {
			return
		}
	}

	// writing the data
	var data []byte
	if data, err = json.Marshal(self.AuctionDataResponse); err != nil {
		return
	}
	if data, err = Util.GzipEncode(data); err != nil {
		return
	}
	dest := fmt.Sprintf("%s/%s.json.gz", folder, realm.Slug)
	if err = ioutil.WriteFile(dest, data, 0777); err != nil {
		return
	}

	return
}
