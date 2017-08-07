package app

type resolver struct {
	getStatusURL      getStatusURLFunc
	getAuctionInfoURL getAuctionInfoURLFunc
	getAuctionsURL    getAuctionsURLFunc
}
