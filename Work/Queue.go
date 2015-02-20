package Work

import (
	"errors"
	"fmt"
	"github.com/ihsw/go-download/Blizzard/Auction"
	"github.com/ihsw/go-download/Blizzard/AuctionData"
	"github.com/ihsw/go-download/Blizzard/CharacterGuild"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Entity/Character"
	"time"
)

type Queue struct {
	DownloadIn               chan Entity.Realm
	ItemizeIn                chan DownloadResult
	ItemizeOut               chan ItemizeResult
	CharacterGuildsResultIn  chan DownloadResult
	CharacterGuildsResultOut chan CharacterGuildsResult
	CacheClient              Cache.Client
}

func (self Queue) DownloadRealms(regionRealms map[int64][]Entity.Realm, totalRealms int) (map[int64][]Entity.Realm, error) {
	var err error

	// formatting the realms to be evenly distributed
	largestRegion := 0
	for _, realms := range regionRealms {
		if len(realms) > largestRegion {
			largestRegion = len(realms)
		}
	}
	formattedRealms := make([]map[int64]Entity.Realm, largestRegion)
	for regionId, realms := range regionRealms {
		for i, realm := range realms {
			if formattedRealms[int64(i)] == nil {
				formattedRealms[int64(i)] = map[int64]Entity.Realm{}
			}
			formattedRealms[int64(i)][regionId] = realm
		}
	}

	// populating the download queue
	for _, realms := range formattedRealms {
		for _, realm := range realms {
			self.DownloadIn <- realm
		}
	}

	// waiting for the results to drain out
	itemizeResults := ItemizeResults{list: []ItemizeResult{}}
	for i := 0; i < totalRealms*2; i++ {
		select {
		case result := <-self.ItemizeOut:
			if result.Err != nil {
				err = errors.New(fmt.Sprintf("itemizeOut %s (%d) had an error (%s)", result.realm.Dump(), result.realm.Id, result.Err.Error()))
				return regionRealms, err
			}

			if result.ResponseFailed {
				continue
			}

			if result.AlreadyChecked {
				continue
			}

			itemizeResults.list = append(itemizeResults.list, result)
		case result := <-self.CharacterGuildsResultOut:
			if result.Err != nil {
				err = errors.New(fmt.Sprintf("characterGuildsResultOut %s (%d) had an error (%s)", result.realm.Dump(), result.realm.Id, err.Error()))
				return regionRealms, err
			}

			fmt.Println(fmt.Sprintf("CharacterGuildsResult for %s was success!", result.realm.Dump()))
		}
	}

	// refresing the region-realms list
	for _, result := range itemizeResults.list {
		resultRealm := result.realm
		resultRegion := resultRealm.Region
		for i, realm := range regionRealms[resultRegion.Id] {
			if realm.Id != resultRealm.Id {
				continue
			}

			regionRealms[result.realm.Region.Id][i] = resultRealm
		}
	}

	/*
		item handling
	*/
	// misc
	itemManager := Entity.ItemManager{Client: self.CacheClient}

	// gathering existing items
	var existingBlizzIds []int64
	if existingBlizzIds, err = itemManager.GetBlizzIds(); err != nil {
		return regionRealms, err
	}

	// gathering new items
	if err = itemManager.PersistAll(itemizeResults.getNewItems(existingBlizzIds)); err != nil {
		return regionRealms, err
	}

	return regionRealms, nil
}

func (self Queue) downloadOut(result DownloadResult) {
	self.ItemizeIn <- result
	self.CharacterGuildsResultIn <- result
}

func (self Queue) DownloadRealm(realm Entity.Realm, skipAlreadyChecked bool) {
	// misc
	realmManager := Entity.NewRealmManager(self.CacheClient)
	result := NewDownloadResult(realm)

	// fetching the auction info
	var (
		auctionResponse *Auction.Response
		err             error
	)
	if auctionResponse, err = Auction.Get(realm, self.CacheClient.ApiKey); err != nil {
		result.Err = errors.New(fmt.Sprintf("Auction.Get() failed (%s)", err.Error()))
		self.downloadOut(result)
		return
	}

	// optionally halting on empty response
	if auctionResponse == nil {
		result.ResponseFailed = true
		self.downloadOut(result)
		return
	}

	file := auctionResponse.Files[0]

	// checking whether the file has already been downloaded
	result.LastModified = time.Unix(file.LastModified/1000, 0)
	if skipAlreadyChecked && !realm.LastDownloaded.IsZero() && (realm.LastDownloaded.Equal(result.LastModified) || realm.LastDownloaded.After(result.LastModified)) {
		realm.LastChecked = time.Now()
		result.AlreadyChecked = true
		self.downloadOut(result)
		return
	}

	// fetching the actual auction data
	if result.AuctionDataResponse = AuctionData.Get(realm, file.Url); result.AuctionDataResponse == nil {
		result.ResponseFailed = true
		self.downloadOut(result)
		return
	}

	// dumping the auction data for parsing after itemize-results are tabulated
	if err = result.dumpData(); err != nil {
		result.Err = errors.New(fmt.Sprintf("DownloadResult.dumpData() failed (%s)", err.Error()))
		self.downloadOut(result)
		return
	}

	// flagging the realm as having been downloaded
	realm.LastDownloaded = result.LastModified
	realm.LastChecked = time.Now()
	realmManager.Persist(realm)
	result.realm = realm

	// queueing it out
	self.downloadOut(result)
}

func (self Queue) ItemizeRealm(downloadResult DownloadResult) {
	// misc
	realm := downloadResult.realm
	result := NewItemizeResult(downloadResult.AuctionDataResult)

	// optionally halting on error
	if downloadResult.Err != nil {
		result.Err = errors.New(fmt.Sprintf("downloadResult had an error (%s)", downloadResult.Err.Error()))
		self.ItemizeOut <- result
		return
	}

	// optionally skipping failed responses or already having been checked
	if result.ResponseFailed || result.AlreadyChecked {
		self.ItemizeOut <- result
		return
	}

	/*
		character handling
	*/
	characterManager := Character.NewManager(realm, self.CacheClient)

	// gathering existing characters
	var (
		existingNames []string
		err           error
	)
	if existingNames, err = characterManager.GetNames(); err != nil {
		result.Err = errors.New(fmt.Sprintf("CharacterManager.GetNames() failed (%s)", err.Error()))
		self.ItemizeOut <- result
		return
	}

	// gathering new characters
	if err = characterManager.PersistAll(downloadResult.getNewCharacters(existingNames)); err != nil {
		result.Err = errors.New(fmt.Sprintf("CharacterManager.PersistAll() failed (%s)", err.Error()))
		self.ItemizeOut <- result
		return
	}

	/*
		item handling
	*/
	result.blizzItemIds = downloadResult.getBlizzItemIds()

	// queueing it out
	self.ItemizeOut <- result
}

func (self Queue) ResolveCharacterGuilds(downloadResult DownloadResult) {
	// misc
	realm := downloadResult.realm
	result := NewCharacterGuildsResult(realm)

	// optionally halting on error
	if downloadResult.Err != nil {
		result.Err = errors.New(fmt.Sprintf("downloadResult had an error (%s)", downloadResult.Err.Error()))
		self.CharacterGuildsResultOut <- result
		return
	}

	// gathering the list of characters
	characterManager := Character.NewManager(realm, self.CacheClient)
	var (
		characters []Character.Character
		err        error
	)
	if characters, err = characterManager.FindAll(); err != nil {
		result.Err = errors.New(fmt.Sprintf("CharacterManager.FindAll() failed (%s)", err.Error()))
		self.CharacterGuildsResultOut <- result
		return
	}

	// going over the characters to gather the guild name
	for _, character := range characters {
		var response *CharacterGuild.Response
		if response, err = CharacterGuild.Get(character, self.CacheClient.ApiKey); err != nil {
			result.Err = errors.New(fmt.Sprintf("CharacterGuild.Get() failed (%s)", err.Error()))
			self.CharacterGuildsResultOut <- result
			return
		}

		if response.IsValid() && response.HasGuild() {
			fmt.Println(fmt.Sprintf("Guild: %s", response.Guild.Name))
		}
	}

	self.CharacterGuildsResultOut <- result
}
