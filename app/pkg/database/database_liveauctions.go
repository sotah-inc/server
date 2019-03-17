package database

import (
	"fmt"

	"github.com/sotah-inc/server/app/pkg/sotah"
)

func liveAuctionsBucketName() []byte {
	return []byte("live-auctions")
}

func liveAuctionsKeyName() []byte {
	return []byte("live-auctions")
}

func liveAuctionsDatabasePath(dirPath string, rea sotah.Realm) string {
	return fmt.Sprintf("%s/%s/%s/live-auctions.db", dirPath, rea.Region.Name, rea.Slug)
}
