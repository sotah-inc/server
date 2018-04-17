package main

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"

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
		verbosity      = app.Flag("verbosity", "Log verbosity").Default("info").Short('v').String()
		dataDir        = app.Flag("data-dir", "Directory to load data files from").Short('d').String()

		apiTestCommand = app.Command(commands.APITest, "For running sotah-api tests.")
		apiCommand     = app.Command(commands.API, "For running sotah-server.")
		apiTestDataDir = apiTestCommand.Flag("data-dir", "Directory to load test fixtures from").Required().Short('d').String()
	)
	cmd := kingpin.MustParse(app.Parse(os.Args[1:]))

	logVerbosity, err := log.ParseLevel(*verbosity)
	if err != nil {
		fmt.Print(err.Error())

		return
	}

	log.SetLevel(logVerbosity)

	log.Info("Starting")

	// loading the config file
	c, err := newConfigFromFilepath(*configFilepath)
	if err != nil {
		log.Fatalf("Could not fetch config: %s\n", err.Error())

		return
	}

	// optionally overriding api key in config
	if len(*apiKey) > 0 {
		log.WithField("api-key", *apiKey).Info("Overriding api key found in config")

		c.APIKey = *apiKey
	}

	// optionally overriding data-dir in config
	if len(*dataDir) > 0 {
		log.WithField("data-dir", *dataDir).Info("Overriding data-dir found in config")

		c.DataDir = *dataDir
	}

	// connecting the messenger
	mess, err := newMessenger(*natsHost, *natsPort)
	if err != nil {
		log.Fatalf("Could not connect messenger: %s\n", err.Error())

		return
	}

	log.WithField("command", cmd).Info("Running command")

	switch cmd {
	case apiTestCommand.FullCommand():
		err := apiTest(c, mess, *apiTestDataDir)
		if err != nil {
			fmt.Printf("Could not run api test command: %s\n", err.Error())
			os.Exit(1)

			return
		}

		os.Exit(0)

		return
	case apiCommand.FullCommand():
		err := api(c, mess)
		if err != nil {
			fmt.Printf("Could not run api command: %s\n", err.Error())
			os.Exit(1)

			return
		}

		os.Exit(0)

		return
	}
}
