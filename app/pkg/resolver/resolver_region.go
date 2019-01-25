package resolver

import (
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/sotah"
)

func (res Resolver) NewStatus(reg sotah.Region) (sotah.Status, error) {
	uri, err := res.AppendAccessToken(res.GetStatusURL(reg.Hostname))
	if err != nil {
		return sotah.Status{}, err
	}

	stat, _, err := blizzard.NewStatusFromHTTP(uri)
	if err != nil {
		return sotah.Status{}, err
	}

	return sotah.Status{Status: stat, Region: reg, Realms: sotah.NewRealms(reg, stat.Realms)}, nil
}
