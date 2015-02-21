package Work

import (
	"encoding/json"
	"fmt"
	"github.com/ihsw/go-download/Blizzard/AuctionData"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Entity/Character"
	"github.com/ihsw/go-download/Util"
	"io/ioutil"
	"os"
	"time"
)

/*
	funcs
*/
func NewDownloadResult(realm Entity.Realm) DownloadResult {
	return DownloadResult{AuctionDataResult: NewAuctionDataResult(realm)}
}

/*
	DownloadResult
*/
type DownloadResult struct {
	AuctionDataResult
	AuctionDataResponse *AuctionData.Response
	LastModified        time.Time
}

func (self DownloadResult) getBlizzItemIds() []int64 {
	// gather unique blizz item ids
	uniqueBlizzItemIds := make(map[int64]struct{})
	for _, auction := range self.AuctionDataResponse.Auctions.Auctions {
		blizzItemId := auction.Item
		_, valid := uniqueBlizzItemIds[blizzItemId]
		if !valid {
			uniqueBlizzItemIds[blizzItemId] = struct{}{}
		}
	}

	// formatting
	blizzItemIds := make([]int64, len(uniqueBlizzItemIds))
	i := 0
	for blizzItemId, _ := range uniqueBlizzItemIds {
		blizzItemIds[i] = blizzItemId
		i++
	}

	return blizzItemIds
}

func (self DownloadResult) getNewCharacters(existingNames []string) (newCharacters []Character.Character) {
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

func (self DownloadResult) dumpData() (err error) {
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
