package main

import (
	"fmt"
	"log"
	"os"

	"github.com/ihsw/sotah-server/app/commands"

	"gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	// parsing the command flags
	var (
		app            = kingpin.New("sotah-server", "A command-line Blizzard AH client.")
		natsHost       = app.Flag("nats-host", "NATS hostname").Default("localhost").OverrideDefaultFromEnvar("NATS_HOST").Short('h').String()
		natsPort       = app.Flag("nats-port", "NATS port").Default("4222").OverrideDefaultFromEnvar("NATS_PORT").Short('p').Int()
		configFilepath = app.Flag("config", "Relative path to config json").Required().Short('c').String()
		apiKey         = app.Flag("api-key", "Blizzard Mashery API key").OverrideDefaultFromEnvar("API_KEY").String()

		apiTestCommand = app.Command(commands.APITest, "For running sotah-api tests.")
	)
	cmd := kingpin.MustParse(app.Parse(os.Args[1:]))

	// loading the config file
	c, err := newConfigFromFilepath(*configFilepath)
	if err != nil {
		log.Fatalf("Could not fetch config: %s\n", err.Error())

		return
	}

	// optionally overriding api key in config
	if len(*apiKey) > 0 {
		c.APIKey = *apiKey
	}

	// connecting the messenger
	mess, err := newMessenger(*natsHost, *natsPort)
	if err != nil {
		log.Fatalf("Could not connect messenger: %s\n", err.Error())

		return
	}

	switch cmd {
	case apiTestCommand.FullCommand():
		err := apiTest(c, mess)
		if err != nil {
			fmt.Printf("Could not run api test command: %s\n", err.Error())
			os.Exit(1)

			return
		}

		os.Exit(0)

		return
	}
}
