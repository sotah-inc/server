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

type AchievementId int

type CharacterAchievementsCompleted []AchievementId

type CharacterAchievementsCompletedTimestamp []int

type CharacterAchievementsCriteria []int

type CharacterCriteriaQuantity []int

type CharacterCriteriaTimestamp []int

type CharacterCriteriaCreated []int

type CharacterAchievements struct {
	AchievementsCompleted          CharacterAchievementsCompleted          `json:"achievementsCompleted"`
	AchievementsCompletedTimestamp CharacterAchievementsCompletedTimestamp `json:"achievementsCompletedTimestamp"`
	Criteria                       CharacterAchievementsCriteria           `json:"criteria"`
	CriteriaQuantity               CharacterCriteriaQuantity               `json:"criteriaQuantity"`
	CriteriaTimestamp              CharacterCriteriaTimestamp              `json:"criteriaTimestamp"`
	CriteriaCreated                CharacterCriteriaCreated                `json:"criteriaCreated"`
}

type CharacterWithAchievements struct {
	Character
	Achievements CharacterAchievements `json:"achievements"`
}
