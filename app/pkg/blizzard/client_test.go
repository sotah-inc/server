package blizzard

import (
	"testing"

	"github.com/sotah-inc/server/app/pkg/utiltest"
	"github.com/stretchr/testify/assert"
)

func TestClientRefresh(t *testing.T) {
	client := NewClient("", "")

	ts, err := utiltest.ServeFile("../TestData/access-token.json")
	if !assert.Nil(t, err) {
		return
	}

	client, err = client.RefreshFromHTTP(ts.URL)
	if !assert.Nil(t, err) {
		return
	}
	if assert.Equal(t, "xxx", client.accessToken) {
		return
	}
}

func TestAppendAccessToken(t *testing.T) {
	client := NewClient("", "")

	ts, err := utiltest.ServeFile("../TestData/access-token.json")
	if !assert.Nil(t, err) {
		return
	}

	client, err = client.RefreshFromHTTP(ts.URL)
	if !assert.Nil(t, err) {
		return
	}

	dest, err := client.AppendAccessToken("https://google.ca/")
	if !assert.Nil(t, err) {
		return
	}
	if !assert.Equal(t, "https://google.ca/?access_token=xxx", dest) {
		return
	}
}
func TestAppendAccessTokenFail(t *testing.T) {
	client := NewClient("", "")

	if _, err := client.AppendAccessToken("https://google.ca/"); !assert.NotNil(t, err) {
		return
	}
}
