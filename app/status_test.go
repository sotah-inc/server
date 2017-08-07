package app

import (
	"testing"

	"github.com/ihsw/go-download/app/utiltest"
	"github.com/stretchr/testify/assert"
)

func TestNewStatus(t *testing.T) {
	ts, err := utiltest.ServeFile("./TestData/realm-status.json")
	if !assert.Nil(t, err) {
		return
	}

	s, err := newStatus("", func(regionName string) string { return ts.URL })
	if !assert.NotEmpty(t, s.Realms) {
		return
	}
}
