package app

import (
	"testing"

	"github.com/ihsw/go-download/app/utiltest"
	"github.com/stretchr/testify/assert"
)

func TestNewStatusFromHTTP(t *testing.T) {
	ts, err := utiltest.ServeFile("./TestData/realm-status.json")
	if !assert.Nil(t, err) {
		return
	}

	reg := region{}
	s, err := newStatusFromHTTP(
		reg,
		resolver{getStatusURL: func(regionHostname string) string { return ts.URL }},
	)
	if !assert.NotEmpty(t, s.Realms) {
		return
	}

	for _, rea := range s.Realms {
		if !assert.Equal(t, reg.Hostname, rea.region.Hostname) {
			return
		}
	}
}
