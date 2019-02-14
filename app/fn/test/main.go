package test

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/pubsub"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
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
	topic, err := bu.ResolveTopic(string(subjects.Boot))
	if err != nil {
		http.Error(w, "Error getting topic", http.StatusInternalServerError)

		return
	}

	id, err := topic.Publish(r.Context(), &pubsub.Message{Data: []byte("wew")}).Get(r.Context())
	if err != nil {
		http.Error(w, "Error publishing to topic", http.StatusInternalServerError)

		return
	}

	fmt.Fprintf(w, "Published msg: %v", id)
}
