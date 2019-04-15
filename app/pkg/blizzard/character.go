package blizzard

import (
	"github.com/sotah-inc/server/app/pkg/blizzard/characterclass"
	"github.com/sotah-inc/server/app/pkg/blizzard/characterfaction"
	"github.com/sotah-inc/server/app/pkg/blizzard/charactergender"
	"github.com/sotah-inc/server/app/pkg/blizzard/characterrace"
)

type Character struct {
	LastModified        int                               `json:"lastModified"`
	Name                string                            `json:"name"`
	Realm               string                            `json:"realm"`
	Battlegroup         string                            `json:"battlegroup"`
	Class               characterclass.CharacterClass     `json:"class"`
	Race                characterrace.CharacterRace       `json:"race"`
	Gender              charactergender.CharacterGender   `json:"gender"`
	Level               int                               `json:"level"`
	AchievementPoints   int                               `json:"achievementPoints"`
	Thumbnail           string                            `json:"thumbnail"`
	CalcClass           string                            `json:"calcClass"`
	Faction             characterfaction.CharacterFaction `json:"faction"`
	TotalHonorableKills int                               `json:"totalHonorableKills"`
}
