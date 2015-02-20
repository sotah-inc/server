package Work

import (
	"github.com/ihsw/go-download/Entity"
)

/*
	funcs
*/
func NewResult(realm Entity.Realm) Result {
	return Result{realm: realm}
}

/*
	Result
*/
type Result struct {
	Err   error
	realm Entity.Realm
}
