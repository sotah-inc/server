package hell

type Realm struct {
	Slug                       string `firestore:"slug"`
	Downloaded                 int    `firestore:"downloaded"`
	LiveAuctionsReceived       int    `firestore:"live_auctions_received"`
	PricelistHistoriesReceived int    `firestore:"pricelist_histories_received"`
}
