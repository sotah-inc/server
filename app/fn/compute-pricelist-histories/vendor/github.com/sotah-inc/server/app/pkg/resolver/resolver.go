package resolver

import (
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/metric"
)

func NewResolver(bc blizzard.Client, re metric.Reporter) Resolver {
	return Resolver{
		BlizzardClient: bc,
		Reporter:       re,

		GetStatusURL:      blizzard.DefaultGetStatusURL,
		GetAuctionInfoURL: blizzard.DefaultGetAuctionInfoURL,
		GetAuctionsURL:    blizzard.DefaultGetAuctionsURL,
		GetItemURL:        blizzard.DefaultGetItemURL,
		GetItemIconURL:    blizzard.DefaultGetItemIconURL,
		GetItemClassesURL: blizzard.DefaultGetItemClassesURL,
	}
}

type Resolver struct {
	BlizzardClient blizzard.Client
	Reporter       metric.Reporter

	GetStatusURL      blizzard.GetStatusURLFunc
	GetAuctionInfoURL blizzard.GetAuctionInfoURLFunc
	GetAuctionsURL    blizzard.GetAuctionsURLFunc
	GetItemURL        blizzard.GetItemURLFunc
	GetItemIconURL    blizzard.GetItemIconURLFunc
	GetItemClassesURL blizzard.GetItemClassesURLFunc
}

func (r Resolver) AppendAccessToken(destination string) (string, error) {
	return r.BlizzardClient.AppendAccessToken(destination)
}

func (r Resolver) Download(uri string, shouldAppendAccessToken bool) (blizzard.ResponseMeta, error) {
	uri, err := func() (string, error) {
		if !shouldAppendAccessToken {
			return uri, nil
		}

		return r.AppendAccessToken(uri)
	}()
	if err != nil {
		return blizzard.ResponseMeta{}, err
	}

	resp, err := blizzard.Download(uri)
	if resp.RequestDuration > 0 || resp.ConnectionDuration > 0 {
		r.Reporter.Report(metric.Metrics{
			"conn_duration":    int(resp.ConnectionDuration / 1000 / 1000),
			"request_duration": int(resp.RequestDuration / 1000 / 1000),
		})
	}

	return resp, nil
}
