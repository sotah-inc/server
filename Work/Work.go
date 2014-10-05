package Work

import (
	"fmt"
	"github.com/ihsw/go-download/Blizzard/Auction"
	"github.com/ihsw/go-download/Blizzard/AuctionData"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Entity"
	"strings"
	"time"
)

/*
	chan structs
*/
type DownloadResult struct {
	AuctionResponse     Auction.Response
	AuctionDataResponse AuctionData.Response
	DataUrl             string
	Error               error
	Realm               Entity.Realm
}

type ItemizeResult struct {
	Error error
	Realm Entity.Realm
}

/*
	funcs
*/
func DownloadRealm(realm Entity.Realm, cacheClient Cache.Client, out chan DownloadResult) {
	// misc
	realmManager := Entity.RealmManager{Client: cacheClient}
	result := DownloadResult{
		Realm: realm,
	}

	// fetching the auction info
	result.AuctionResponse, result.Error = Auction.Get(realm, cacheClient.ApiKey)
	if result.Error != nil {
		out <- result
		return
	}

	// fetching the actual auction data
	file := result.AuctionResponse.Files[0]
	result.AuctionDataResponse, result.Error = AuctionData.Get(realm, file.Url)
	if result.Error != nil {
		out <- result
		return
	}

	// flagging the realm as having been downloaded
	realm.LastDownloaded = time.Now()
	realmManager.Persist(realm)

	// queueing it out
	out <- result
}

func ItemizeRealm(downloadResult DownloadResult, cacheClient Cache.Client, out chan ItemizeResult) {
	// misc
	realm := downloadResult.Realm
	result := ItemizeResult{
		Realm: realm,
	}

	// optionally halting on error
	if downloadResult.Error != nil {
		result.Error = downloadResult.Error
		out <- result
		return
	}

	// gathering up unique sellers
	data := downloadResult.AuctionDataResponse
	auctionGroups := [][]AuctionData.Auction{
		data.Alliance.Auctions,
		data.Horde.Auctions,
		data.Neutral.Auctions,
	}
	realmSellers := make(map[string]map[string]string)
	for _, auctions := range auctionGroups {
		for _, auction := range auctions {
			seller := auction.Owner
			sellerRealm := auction.OwnerRealm

			_, valid := realmSellers[sellerRealm]
			if !valid {
				realmSellers[sellerRealm] = make(map[string]string)
			}

			realmSellers[sellerRealm][seller] = seller
		}
	}

	nameCollisions := make(map[string]map[string]string)
	for sellerRealm, sellers := range realmSellers {
		for _, seller := range sellers {
			_, valid := nameCollisions[seller]
			if !valid {
				nameCollisions[seller] = make(map[string]string)
			}
			nameCollisions[seller][sellerRealm] = sellerRealm
		}
	}

	for seller, sellerRealms := range nameCollisions {
		if len(sellerRealms) == 1 {
			continue
		}
		a := make([]string, len(sellerRealms))
		i := 0
		for _, sellerRealm := range sellerRealms {
			a[i] = sellerRealm
			i++
		}
		fmt.Println(fmt.Sprintf("%s is found on %s", seller, strings.Join(a, ", ")))
	}

	// queueing it out
	out <- result
}
