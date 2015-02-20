package CharacterGuild

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ihsw/go-download/Entity/Character"
	"github.com/ihsw/go-download/Util"
)

/*
	Response
*/
type Response struct {
	LastModified        int64
	Name                string
	Realm               string
	Battlegroup         string
	Class               int64
	Race                int64
	Level               int64
	AchievementPoints   int64
	Thumbnail           string
	CalcClass           string
	Guild               Guild
	TotalHonorableKills int64
}

/*
	Guild
*/
type Guild struct {
	Name              string
	Realm             string
	Battlegroup       string
	Members           int64
	AchievementPoints int64
	Emblem            Emblem
}

/*
	Emblem
*/
type Emblem struct {
	Icon            int64
	IconColor       string
	Border          int64
	BorderColor     string
	BackgroundColor string
}

const URL_FORMAT = "https://%s/wow/character/%s/%s?fields=guild&apikey=%s"

/*
	funcs
*/
func Get(character Character.Character, apiKey string) (response *Response, err error) {
	url := fmt.Sprintf(URL_FORMAT,
		character.Realm.Region.Host,
		character.Realm.Slug,
		character.Name,
		apiKey,
	)
	var b []byte
	if b, err = Util.Download(url); err != nil {
		err = errors.New(fmt.Sprintf("Util.Download() for %s failed (%s)", url, err.Error()))
		return
	}

	if err = json.Unmarshal(b, &response); err != nil {
		return nil, err
	}

	return response, nil
}
