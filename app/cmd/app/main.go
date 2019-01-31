package main

import (
	"os"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/cmd/app/commands"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/logging"
	"github.com/sotah-inc/server/app/pkg/logging/stackdriver"
	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/resolver"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/store"
	"github.com/twinj/uuid"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

type commandMap map[string]func() error

// ID represents this run's unique id
var ID uuid.UUID

func main() {
	// assigning global ID
	ID = uuid.NewV4()

	// parsing the command flags
	var (
		app            = kingpin.New("sotah-server", "A command-line Blizzard AH client.")
		natsHost       = app.Flag("nats-host", "NATS hostname").Default("localhost").Envar("NATS_HOST").Short('h').String()
		natsPort       = app.Flag("nats-port", "NATS port").Default("4222").Envar("NATS_PORT").Short('p').Int()
		configFilepath = app.Flag("config", "Relative path to config json").Required().Short('c').String()
		clientID       = app.Flag("client-id", "Blizzard API Client ID").Envar("CLIENT_ID").String()
		clientSecret   = app.Flag("client-secret", "Blizzard API Client Secret").Envar("CLIENT_SECRET").String()
		verbosity      = app.Flag("verbosity", "Log verbosity").Default("info").Short('v').String()
		cacheDir       = app.Flag("cache-dir", "Directory to cache data files to").Required().String()
		projectID      = app.Flag("project-id", "GCloud Storage Project ID").Default("").Envar("PROJECT_ID").String()

		apiTestCommand            = app.Command(commands.APITest, "For running sotah-api tests.")
		apiTestDataDir            = apiTestCommand.Flag("data-dir", "Directory to load test fixtures from").Required().Short('d').String()
		apiCommand                = app.Command(commands.API, "For running sotah-server.")
		syncItemsCommand          = app.Command(commands.SyncItems, "For syncing items in gcloud storage to local disk.")
		liveAuctionsCommand       = app.Command(commands.LiveAuctions, "For in-memory storage of current auctions.")
		pricelistHistoriesCommand = app.Command(commands.PricelistHistories, "For on-disk storage of pricelist histories.")
		pruneStoreCommand         = app.Command(commands.PruneStore, "For pruning gcloud store of non-primary region auctions.")
	)
	cmd := kingpin.MustParse(app.Parse(os.Args[1:]))

	logVerbosity, err := logrus.ParseLevel(*verbosity)
	if err != nil {
		logging.WithField("error", err.Error()).Fatal("Could not parse log level")

		return
	}
	logging.SetLevel(logVerbosity)

	// loading the config file
	c, err := sotah.NewConfigFromFilepath(*configFilepath)
	if err != nil {
		logging.WithFields(logrus.Fields{
			"error":    err.Error(),
			"filepath": *configFilepath,
		}).Fatal("Could not fetch config")

		return
	}

	// optionally adding stackdriver hook
	if c.UseGCloud {
		stackdriverHook, err := stackdriver.NewHook(*projectID, cmd)
		if err != nil {
			logging.WithFields(logrus.Fields{
				"error":     err.Error(),
				"projectID": projectID,
			}).Fatal("Could not create new stackdriver logrus hook")
		}

		logging.AddHook(stackdriverHook)
	}
	logging.Info("Starting")

	// connecting the messenger
	mess, err := messenger.NewMessenger(*natsHost, *natsPort)
	if err != nil {
		logging.WithFields(logrus.Fields{
			"error": err.Error(),
			"host":  *natsHost,
			"port":  *natsPort,
		}).Fatal("Could not connect messenger")

		return
	}

	// connecting storage
	stor := store.Store{}
	if c.UseGCloud {
		stor, err = store.NewStore(*projectID)
		if err != nil {
			logging.WithFields(logrus.Fields{
				"error":     err.Error(),
				"projectId": *projectID,
			}).Fatal("Could not connect store")

			return
		}
	}

	// connecting the blizzard client
	blizzardClient := blizzard.Client{}
	if len(*clientID) > 0 && len(*clientSecret) > 0 {
		blizzardClient, err = blizzard.NewClient(*clientID, *clientSecret)
		if err != nil {
			logging.WithField("error", err.Error()).Fatal("Could not create blizzard client")

			return
		}
	}

	// creating a resolver
	resolv := resolver.NewResolver()
	resolv.BlizzardClient = blizzardClient

	logging.WithField("command", cmd).Info("Running command")

	cMap := commandMap{
		apiTestCommand.FullCommand(): func() error {
			return apiTest(c, mess, stor, *apiTestDataDir)
		},
		apiCommand.FullCommand(): func() error {
			return api(c, mess, stor)
		},
		syncItemsCommand.FullCommand(): func() error {
			return syncItems(c, stor)
		},
		liveAuctionsCommand.FullCommand(): func() error {
			return liveAuctions(c, mess, stor)
		},
		pricelistHistoriesCommand.FullCommand(): func() error {
			return pricelistHistories(c, mess, stor)
		},
		pruneStoreCommand.FullCommand(): func() error {
			return pruneStore(c, mess, stor)
		},
	}
	cmdFunc, ok := cMap[cmd]
	if !ok {
		logging.WithField("command", cmd).Fatal("Invalid command")

		return
	}

	if c.UseGCloud {
		stackdriverHook, err := stackdriver.NewHook(*projectID, cmd)
		if err != nil {
			logging.WithFields(logrus.Fields{
				"error":     err.Error(),
				"projectID": projectID,
			}).Fatal("Could not create new stackdriver logrus hook")
		}

		logging.ResetLogger(logVerbosity, stackdriverHook)
	}

	if err := cmdFunc(); err != nil {
		logging.WithFields(logrus.Fields{
			"error":   err.Error,
			"command": cmd,
		}).Fatal("Failed to execute command")
	}
}
