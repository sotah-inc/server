package main

import (
	"fmt"
	"os"

	"github.com/ihsw/sotah-server/app/commands"
	log "github.com/sirupsen/logrus"
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
		cacheDir       = app.Flag("cache-dir", "Directory to cache data files to").Required().String()
		projectID      = app.Flag("project-id", "GCloud Storage Project ID").Default("").OverrideDefaultFromEnvar("PROJECT_ID").String()

		apiTestCommand      = app.Command(commands.APITest, "For running sotah-api tests.")
		apiTestDataDir      = apiTestCommand.Flag("data-dir", "Directory to load test fixtures from").Required().Short('d').String()
		apiCommand          = app.Command(commands.API, "For running sotah-server.")
		syncItemsCommand    = app.Command(commands.SyncItems, "For syncing items in gcloud storage to local disk.")
		liveAuctionsCommand = app.Command(commands.LiveAuctions, "For in-memory storage of current auctions.")
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

	// optionally overriding cache-dir in config
	if len(*cacheDir) > 0 {
		log.WithField("cache-dir", *cacheDir).Info("Overriding cache-dir found in config")

		c.CacheDir = *cacheDir
	}

	// validating the cache dir
	if c.CacheDir == "" {
		log.Fatal("Cache-dir cannot be blank")

		return
	}

	// connecting the messenger
	mess, err := newMessenger(*natsHost, *natsPort)
	if err != nil {
		log.Fatalf("Could not connect messenger: %s\n", err.Error())

		return
	}

	// connecting storage
	stor := store{}
	if c.UseGCloudStorage {
		stor, err = newStore(*projectID)
		if err != nil {
			log.Fatalf("Could not connect store: %s\n", err.Error())

			return
		}
	}

	log.WithField("command", cmd).Info("Running command")

	switch cmd {
	case apiTestCommand.FullCommand():
		err := apiTest(c, mess, stor, *apiTestDataDir)
		if err != nil {
			fmt.Printf("Could not run api test command: %s\n", err.Error())
			os.Exit(1)

			return
		}

		os.Exit(0)

		return
	case apiCommand.FullCommand():
		err := api(c, mess, stor)
		if err != nil {
			fmt.Printf("Could not run api command: %s\n", err.Error())
			os.Exit(1)

			return
		}

		os.Exit(0)

		return
	case syncItemsCommand.FullCommand():
		err := syncItems(c, stor)
		if err != nil {
			fmt.Printf("Could not run sync-items command: %s\n", err.Error())
			os.Exit(1)

			return
		}

		os.Exit(0)

		return
	case liveAuctionsCommand.FullCommand():
		err := liveAuctions(c, mess, stor)
		if err != nil {
			fmt.Printf("Could not run live-auctions command: %s\n", err.Error())
			os.Exit(1)

			return
		}

		os.Exit(0)

		return
	}
}
