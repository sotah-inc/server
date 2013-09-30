package Region

import (
	"github.com/ihsw/go-download/Cache/Locale"
	"github.com/ihsw/go-download/Config"
)

type Data struct {
	Name    string
	Host    string
	Locales map[int64]Locale.Data
}

func New(regionConfig Config.Region) Data {
	region := Data{
		Name: regionConfig.Name,
		Host: regionConfig.Host,
	}
	return r
}

func NewFromConfig(regionConfigs []Config.Region) map[int64]Data {
	var regions []Data
	for _, region := range regionConfigs {
		regions = append(r, New(region))
	}
	return regions
}
