package Work

import (
	"github.com/ihsw/go-download/Entity"
)

/*
	funcs
*/
func NewRealmResult(realm Entity.Realm) RealmResult {
	return RealmResult{realm: realm}
}

/*
	Result
*/
type RealmResult struct {
	Result
	realm Entity.Realm
}
