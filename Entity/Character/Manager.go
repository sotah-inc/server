package Character

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Entity"
	"github.com/ihsw/go-download/Util"
	"strconv"
)

/*
	funcs
*/
func characterNameKey(realm Entity.Realm, name string) string {
	return fmt.Sprintf("realm:%d:character:%s:id", realm.Id, Util.Md5Encode(name))
}

/*
	manager
*/
type Manager struct {
	Client Cache.Client
	Realm  Entity.Realm
}

func (self Manager) Namespace() string { return fmt.Sprintf("realm:%d:character", self.Realm.Id) }

func (self Manager) PersistAll(existingCharacters []Character, newCharacters []Character) (characters []Character, err error) {
	m := self.Client.Main

	// ids
	var ids []int64
	if ids, err = m.IncrAll(fmt.Sprintf("realm:%d:character_id", self.Realm.Id), len(newCharacters)); err != nil {
		return
	}
	for i, id := range ids {
		newCharacters[i].Id = id
	}

	// data
	values := make([]Cache.PersistValue, len(newCharacters))
	for i, character := range newCharacters {
		bucketKey, subKey := Cache.GetBucketKey(character.Id, self.Namespace())

		var s string
		if s, err = character.marshal(); err != nil {
			return
		}

		values[i] = Cache.PersistValue{
			BucketKey: bucketKey,
			SubKey:    subKey,
			Value:     s,
		}
	}
	if err = m.PersistAll(values); err != nil {
		return
	}

	// etc
	newCharacterIds := make([]string, len(newCharacters))
	newNames := make([]string, len(newCharacters))
	hashedNameKeys := map[string]string{}
	for i, character := range newCharacters {
		id := strconv.FormatInt(character.Id, 10)
		name := character.Name

		newCharacterIds[i] = id
		newNames[i] = name
		hashedNameKeys[characterNameKey(self.Realm, name)] = id
	}
	if err = m.RPushAll(fmt.Sprintf("realm:%d:character_ids", self.Realm.Id), newCharacterIds); err != nil {
		return
	}
	if err = m.SAddAll(fmt.Sprintf("realm:%d:character_names", self.Realm.Id), newNames); err != nil {
		return
	}
	if err = m.SetAll(hashedNameKeys); err != nil {
		return
	}

	// merging them together
	characters = make([]Character, len(existingCharacters)+len(newCharacters))
	i := 0
	for _, character := range existingCharacters {
		characters[i] = character
		i++
	}
	for _, character := range newCharacters {
		characters[i] = character
		i++
	}

	return characters, nil
}

func (self Manager) unmarshal(v string) (character Character, err error) {
	if v == "" {
		return
	}

	// json
	var characterJson CharacterJson
	b := []byte(v)
	err = json.Unmarshal(b, &characterJson)
	if err != nil {
		return
	}

	// initial
	character = Character{
		Id:   characterJson.Id,
		Name: characterJson.Name,
	}

	// resolving the realm
	realmManager := Entity.RealmManager{Client: self.Client}
	realm, err := realmManager.FindOneById(characterJson.RealmId)
	if err != nil {
		return
	}
	if !realm.IsValid() {
		err = errors.New(fmt.Sprintf("Realm #%d could not be found!", characterJson.RealmId))
		return
	}
	character.Realm = realm

	return character, nil
}

func (self Manager) unmarshalAll(values []string) (characters []Character, err error) {
	characters = make([]Character, len(values))
	for i, v := range values {
		characters[i], err = self.unmarshal(v)
		if err != nil {
			return
		}
	}
	return
}

func (self Manager) FindAll() (characters []Character, err error) {
	main := self.Client.Main

	// fetching ids
	ids, err := main.FetchIds(fmt.Sprintf("realm:%d:character_ids", self.Realm.Id), 0, -1)
	if err != nil {
		return
	}

	// fetching the values
	var values []string
	values, err = main.FetchFromIds(self, ids)
	if err != nil {
		return
	}

	return self.unmarshalAll(values)
}

func (self Manager) FindOneByName(name string) (character Character, err error) {
	var v string
	v, err = self.Client.Main.FetchFromKey(self, characterNameKey(self.Realm, name))
	if err != nil {
		return
	}

	return self.unmarshal(v)
}

func (self Manager) NameExists(name string) (exists bool, err error) {
	return self.Client.Main.SIsMember(fmt.Sprintf("realm:%d:character_names", self.Realm.Id), name)
}

func (self Manager) NamesExist(names []string) (exists []bool, err error) {
	return self.Client.Main.SIsMemberAll(fmt.Sprintf("realm:%d:character_names", self.Realm.Id), names)
}

func (self Manager) GetNames() (names []string, err error) {
	return self.Client.Main.SMembers(fmt.Sprintf("realm:%d:character_names", self.Realm.Id))
}

func (self Manager) GetIds() (ids []int64, err error) {
	if ids, err = self.Client.Main.FetchIds(fmt.Sprintf("realm:%d:character_ids", self.Realm.Id), 0, -1); err != nil {
		return
	}

	return
}

func (self Manager) GetLastId() (id int64, err error) {
	var v string
	if v, err = self.Client.Main.Get(fmt.Sprintf("realm:%d:character_id", self.Realm.Id)); err != nil {
		return
	}

	if len(v) == 0 {
		return 0, nil
	}

	var i int
	if i, err = strconv.Atoi(v); err != nil {
		return
	}
	return int64(i), nil
}
