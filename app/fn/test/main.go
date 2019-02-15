package test

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

var projectId = os.Getenv("GCP_PROJECT")
var busClient bus.Client

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-test")
	if err != nil {
		log.Fatalf("Failed to create new bus: %s", err.Error())

		return
	}
}

func HelloHTTP(w http.ResponseWriter, r *http.Request) {
	msg, err := busClient.RequestFromTopic(string(subjects.Boot), "world", 5*time.Second)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error sending boot request: %s", err.Error()), http.StatusInternalServerError)

		return
	}

	if msg.Code != codes.Ok {
		http.Error(w, fmt.Sprintf("Response was not Ok: %s", msg.Err), http.StatusInternalServerError)

		return
	}

	fmt.Fprintf(w, "Published msg: %v", msg.Data)
}
