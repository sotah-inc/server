package Work

import (
	"github.com/ihsw/go-download/Entity"
)

/*
	funcs
*/
func NewAuctionDataResult(realm Entity.Realm) AuctionDataResult {
	return AuctionDataResult{RealmResult: NewRealmResult(realm)}
}

/*
	AuctionDataResult
*/
type AuctionDataResult struct {
	RealmResult
	ResponseFailed bool
	AlreadyChecked bool
}

func (self AuctionDataResult) CanContinue() bool {
	return !self.AlreadyChecked && !self.ResponseFailed
}
