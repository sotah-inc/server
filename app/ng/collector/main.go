package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

var projectId = os.Getenv("GOOGLE_CLOUD_PROJECT")
var busClient bus.Client

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-auctions-collector")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)

		return
	}

	// cronHeader := r.Header.Get("X-Appengine-Cron")
	// if cronHeader != "true" {
	// 	w.WriteHeader(http.StatusUnauthorized)
	// 	fmt.Fprint(w, "Unauthorized")
	//
	// 	return
	// }

	topic, err := busClient.ResolveTopic(string(subjects.AuctionsCollectorCompute))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, fmt.Sprintf("Failed to resolve collect-auctions topic: %s", err.Error()))

		return
	}

	reply, err := busClient.RequestFromTopic(string(subjects.AuctionsCollectorCompute), "", 5*time.Second)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, fmt.Sprintf("Failed to publish message to collect-auctions topic: %s", err.Error()))

		return
	}

	fmt.Fprint(w, reply.Data)
}

func main() {
	http.HandleFunc("/", indexHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
		log.Printf("Defaulting to port %s", port)
	}

	log.Printf("Listening on port %s", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
}
