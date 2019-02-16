package test2

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/sotah-inc/server/app/pkg/bus"
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

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func HelloPubSub(_ context.Context, m PubSubMessage) error {
	var in bus.Message
	if err := json.Unmarshal(m.Data, &in); err != nil {
		return err
	}

	response := bus.NewMessage()
	response.Data = fmt.Sprintf("Hello, %s!", m.Data)
	_, err := busClient.ReplyTo(in, response)
	if err != nil {
		return err
	}

	return nil
}
