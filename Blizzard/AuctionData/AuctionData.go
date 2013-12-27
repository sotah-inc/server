package AuctionData

/*
	misc
*/
type Response struct {
	Realm    Realm
	Alliance Auctions
	Horde    Auctions
	Neutral  Auctions
}

type Realm struct {
	Name string
	Slug string
}

type Auctions struct {
	Auctions []Auction
}

type Auction struct {
	Auc        uint64
	Item       uint64
	Owner      string
	OwnerRealm string
	Bid        uint64
	Buyout     uint64
	Quantity   uint64
	TimeLeft   string
	Rand       uint64
	Seed       uint64
}
