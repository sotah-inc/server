package DownloadRealm

import (
	"encoding/json"
	"fmt"
	"github.com/ihsw/go-download/Blizzard/AuctionData"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Queue"
	"github.com/ihsw/go-download/Util"
	"io/ioutil"
	"os"
)

/*
	funcs
*/
func DoWork(in chan Entity.Realm, process func(Entity.Realm) Job) chan Job {
	out := make(chan Job)

	worker := func() {
		for realm := range in {
			out <- process(realm)
		}
	}
	postWork := func() { close(out) }
	Queue.Work(4, worker, postWork)

	return out
}

/*
	Job
*/
func NewJob(realm Entity.Realm) Job {
	return Job{AuctionDataJob: Queue.NewAuctionDataJob(realm)}
}

type Job struct {
	Queue.AuctionDataJob
	AuctionDataResponse *AuctionData.Response
}

func (self Job) DumpData() (err error) {
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
