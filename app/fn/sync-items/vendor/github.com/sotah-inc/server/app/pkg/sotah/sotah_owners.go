package sotah

import (
	"encoding/json"
	"regexp"
	"strings"
)

type OwnerName string

type Owner struct {
	Name           OwnerName `json:"name"`
	NormalizedName string    `json:"normalized_name"`
}

func NewOwnersFromAuctions(aucs MiniAuctionList) (Owners, error) {
	ownerNamesMap := map[OwnerName]struct{}{}
	for _, ma := range aucs {
		ownerNamesMap[ma.Owner] = struct{}{}
	}

	reg, err := regexp.Compile("[^a-z0-9 ]+")
	if err != nil {
		return Owners{}, err
	}

	ownerList := make([]Owner, len(ownerNamesMap))
	i := 0
	for ownerNameValue := range ownerNamesMap {
		ownerList[i] = Owner{
			Name:           ownerNameValue,
			NormalizedName: reg.ReplaceAllString(strings.ToLower(string(ownerNameValue)), ""),
		}
		i++
	}

	return Owners{Owners: ownerList}, nil
}

func NewOwners(payload []byte) (Owners, error) {
	o := &Owners{}
	if err := json.Unmarshal(payload, &o); err != nil {
		return Owners{}, err
	}

	return *o, nil
}

type Owners struct {
	Owners ownersList `json:"owners"`
}

type ownersList []Owner

func (ol ownersList) Limit() ownersList {
	listLength := len(ol)
	if listLength > 10 {
		listLength = 10
	}

	out := make(ownersList, listLength)
	for i := 0; i < listLength; i++ {
		out[i] = ol[i]
	}

	return out
}

func (ol ownersList) Filter(query string) ownersList {
	lowerQuery := strings.ToLower(query)
	matches := ownersList{}
	for _, o := range ol {
		if !strings.Contains(strings.ToLower(string(o.Name)), lowerQuery) {
			continue
		}

		matches = append(matches, o)
	}

	return matches
}

type OwnersByName ownersList

func (by OwnersByName) Len() int           { return len(by) }
func (by OwnersByName) Swap(i, j int)      { by[i], by[j] = by[j], by[i] }
func (by OwnersByName) Less(i, j int) bool { return by[i].Name < by[j].Name }
