package main

type resolver struct {
	getStatusURL      getStatusURLFunc
	getAuctionInfoURL getAuctionInfoURLFunc
	getAuctionsURL    getAuctionsURLFunc
}
