package Work

import (
	"github.com/ihsw/go-download/Entity"
)

/*
	funcs
*/
func NewCharacterGuildResult(realm Entity.Realm) CharacterGuildResult {
	return CharacterGuildResult{Result: NewResult(realm)}
}

/*
	CharacterGuildResult
*/
type CharacterGuildResult struct {
	Result
}
