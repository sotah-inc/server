package Character

import (
	"encoding/json"
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

func NewManager(realm Entity.Realm, client Cache.Client) Manager {
	return Manager{
		Realm:        realm,
		RealmManager: Entity.NewRealmManager(realm.Region, client),
	}
}

/*
	manager
*/
type Manager struct {
	Realm        Entity.Realm
	RealmManager Entity.RealmManager
}

func (self Manager) Namespace() string { return fmt.Sprintf("realm:%d:character", self.Realm.Id) }

func (self Manager) Client() Cache.Client { return self.RealmManager.Client() }

func (self Manager) PersistAll(newCharacters []Character) (err error) {
	m := self.Client().Main

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

	return nil
}

func (self Manager) unmarshal(v string) (character Character, err error) {
	var characters []Character
	if characters, err = self.unmarshalAll([]string{v}); err != nil {
		return
	}

	if len(characters) == 0 {
		return
	}

	return characters[0], nil
}

func (self Manager) unmarshalAll(values []string) (characters []Character, err error) {
	characters = make([]Character, len(values))

	// resolving the characters
	realmIds := []int64{}
	realmKeysBack := map[int64][]int64{}
	for i, v := range values {
		if len(v) == 0 {
			continue
		}

		// json
		characterJson := CharacterJson{}
		if err = json.Unmarshal([]byte(v), &characterJson); err != nil {
			return
		}

		// initial
		characters[i] = Character{
			Id:        characterJson.Id,
			Name:      characterJson.Name,
			GuildName: characterJson.GuildName,
		}

		// realm
		realmId := characterJson.RealmId
		if _, ok := realmKeysBack[realmId]; !ok {
			realmKeysBack[realmId] = []int64{int64(i)}
			realmIds = append(realmIds, realmId)
		} else {
			realmKeysBack[realmId] = append(realmKeysBack[realmId], int64(i))
		}
	}

	// resolving the realms
	var realms []Entity.Realm
	if realms, err = self.RealmManager.FindByIds(realmIds); err != nil {
		return
	}
	for _, realm := range realms {
		for _, i := range realmKeysBack[realm.Id] {
			characters[i].Realm = realm
		}
	}
	return
}

func (self Manager) FindAll() (characters []Character, err error) {
	main := self.Client().Main

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
	v, err = self.Client().Main.FetchFromKey(self, characterNameKey(self.Realm, name))
	if err != nil {
		return
	}

	return self.unmarshal(v)
}

func (self Manager) NameExists(name string) (exists bool, err error) {
	return self.Client().Main.SIsMember(fmt.Sprintf("realm:%d:character_names", self.Realm.Id), name)
}

func (self Manager) NamesExist(names []string) (exists []bool, err error) {
	return self.Client().Main.SIsMemberAll(fmt.Sprintf("realm:%d:character_names", self.Realm.Id), names)
}

func (self Manager) GetNames() (names []string, err error) {
	return self.Client().Main.SMembers(fmt.Sprintf("realm:%d:character_names", self.Realm.Id))
}

func (self Manager) GetIds() (ids []int64, err error) {
	if ids, err = self.Client().Main.FetchIds(fmt.Sprintf("realm:%d:character_ids", self.Realm.Id), 0, -1); err != nil {
		return
	}

	return
}

func (self Manager) GetLastId() (id int64, err error) {
	var v string
	if v, err = self.Client().Main.Get(fmt.Sprintf("realm:%d:character_id", self.Realm.Id)); err != nil {
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
