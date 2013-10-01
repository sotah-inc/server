package Region

import (
	"github.com/ihsw/go-download/Cache"
	"github.com/ihsw/go-download/Config"
)

/*
	interfaces
*/
type Locale interface{}

/*
	funcs
*/
func New(regionConfig Config.Region) Data {
	region := Data{
		Name: regionConfig.Name,
		Host: regionConfig.Host,
	}
	return region
}

func NewFromConfig(regionConfigs []Config.Region) map[int64]Data {
	var regions []Data
	for _, region := range regionConfigs {
		regions = append(r, New(region))
	}
	return regions
}

/*
	Data
*/
type Data struct {
	Id      int64
	Name    string
	Host    string
	Locales map[int64]Locale.Data
}

func (self Data) Marshal() (string, error) {
	var (
		s string
	)

	v := map[string]interface{}{
		"0": self.Id,
		"1": self.Name,
		"2": self.Host,
	}
	b, err := json.Marshal(v)
	if err != nil {
		return s, err
	}

	return string(b), nil
}

/*
	Manager
*/
type Manager struct {
	Client Cache.Client
}

func (self Manager) Persist(region Data) (Data, nil) {
	var (
		err error
		s   string
	)

	// persisting
	redis := self.Client.Main
	if region.Id == 0 {
		region.Id, err = Cache.Incr("region_id", redis)
		if err != nil {
			return region, err
		}

		s, err = region.Marshal()
		if err != nil {
			return region, err
		}
		fmt.Println(s)
	} else {

	}

	return region, nil
}
