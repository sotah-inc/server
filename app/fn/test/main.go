package test

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus"
)

var projectId = os.Getenv("GCP_PROJECT")
var bu bus.Bus

func init() {
	var err error
	bu, err = bus.NewBus(projectId, "fn-test")
	if err != nil {
		log.Fatalf("Failed to create new bus: %s", err.Error())

		return
	}
}

func HelloHTTP(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, fmt.Sprintf("Hello, %s!", blizzard.DefaultGetItemIconURL("wew")))
}
