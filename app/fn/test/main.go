package test

import (
	"fmt"
	"net/http"

	"github.com/sotah-inc/server/app/pkg/blizzard"
)

func HelloHTTP(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, fmt.Sprintf("Hello, %s!", blizzard.DefaultGetItemIconURL("wew")))
}
