package Work

import (
	"github.com/ihsw/go-download/Entity"
)

/*
	funcs
*/
func NewCharacterGuildsResult(realm Entity.Realm) CharacterGuildsResult {
	return CharacterGuildsResult{RealmResult: NewRealmResult(realm)}
}

/*
	CharacterGuildsResult
*/
type CharacterGuildsResult struct {
	RealmResult
}
