package test2

import (
	"context"
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

func HelloPubSub(ctx context.Context, m bus.Message) error {
	response := bus.NewMessage()
	response.Data = fmt.Sprintf("Hello, %s!", m.Data)
	_, err := busClient.ReplyTo(m, response)
	if err != nil {
		return err
	}

	return nil
}
