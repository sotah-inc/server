package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/pubsub"
	"github.com/sotah-inc/server/app/pkg/bus"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
)

var projectId = os.Getenv("GOOGLE_CLOUD_PROJECT")

var busClient bus.Client
var collectComputeTopic *pubsub.Topic
var auctionsCleanupTopic *pubsub.Topic

func init() {
	var err error
	busClient, err = bus.NewClient(projectId, "fn-auctions-collector")
	if err != nil {
		log.Fatalf("Failed to create new bus client: %s", err.Error())

		return
	}

	collectComputeTopic, err = busClient.FirmTopic(string(subjects.AuctionsCollectorCompute))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return
	}
	auctionsCleanupTopic, err = busClient.FirmTopic(string(subjects.AuctionsCleanup))
	if err != nil {
		log.Fatalf("Failed to get firm topic: %s", err.Error())

		return
	}
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)

		return
	}

	cronHeader := r.Header.Get("X-Appengine-Cron")
	if cronHeader != "true" {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, "Unauthorized")

		return
	}

	msg := bus.NewMessage()
	if _, err := busClient.Publish(collectComputeTopic, msg); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, fmt.Sprintf("Failed to publish message to collect-auctions topic: %s", err.Error()))

		return
	}

	fmt.Fprint(w, "Hello, indexHandler()!")
}

func cleanHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/clean" {
		http.NotFound(w, r)

		return
	}

	cronHeader := r.Header.Get("X-Appengine-Cron")
	if cronHeader != "true" {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, "Unauthorized")

		return
	}

	msg := bus.NewMessage()
	if _, err := busClient.Publish(auctionsCleanupTopic, msg); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, fmt.Sprintf("Failed to publish message to auctions-cleanup topic: %s", err.Error()))

		return
	}

	fmt.Fprint(w, "Hello, cleanHandler()!")
}

func main() {
	http.HandleFunc("/", indexHandler)
	http.HandleFunc("/clean", cleanHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
		log.Printf("Defaulting to port %s", port)
	}

	log.Printf("Listening on port %s", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
}
