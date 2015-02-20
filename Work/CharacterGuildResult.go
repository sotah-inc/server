package Work

import (
	"github.com/ihsw/go-download/Blizzard/CharacterGuild"
	"github.com/ihsw/go-download/Entity/Character"
)

type CharacterGuildResult struct {
	Result
	Response  *CharacterGuild.Response
	Character Character.Character
}
