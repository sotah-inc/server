package Region

import (
	"github.com/ihsw/go-download/Cache/Locale"
	"github.com/ihsw/go-download/Config"
)

type Region struct {
	Name    string
	Host    string
	Locales []Locale.Locale
}

func New(region Config.Region) Region {
	r := Region{
		Name:    region.Name,
		Host:    region.Host,
		Locales: Locale.NewFromList(region.Locales),
	}
	return r
}

func NewFromList(regions []Config.Region) []Region {
	var r []Region
	for _, region := range regions {
		r = append(r, New(region))
	}
	return r
}
