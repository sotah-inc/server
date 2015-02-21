package Work

import (
	"github.com/ihsw/go-download/Entity"
)

/*
	funcs
*/
func NewRealmResult(realm Entity.Realm) RealmResult {
	return RealmResult{Realm: realm}
}

/*
	Result
*/
type RealmResult struct {
	Result
	Realm Entity.Realm
}
