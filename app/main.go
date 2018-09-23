package main

import (
	"fmt"
	"net"
	"os"

	"github.com/twinj/uuid"

	logrusstash "github.com/bshuster-repo/logrus-logstash-hook"
	"github.com/ihsw/sotah-server/app/commands"
	"github.com/ihsw/sotah-server/app/logging"
	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

type commandMap map[string]func() error

// ID represents this run's unique id
var ID uuid.Uuid

func main() {
	// assigning global ID
	uuid.Init()
	ID = uuid.NewV4()

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
		logstashHost   = app.Flag("logstash-host", "Logstash host").OverrideDefaultFromEnvar("LOGSTASH_HOST").String()
		logstashPort   = app.Flag("logstash-port", "Logstash port").OverrideDefaultFromEnvar("LOGSTASH_PORT").Int()

		apiTestCommand            = app.Command(commands.APITest, "For running sotah-api tests.")
		apiTestDataDir            = apiTestCommand.Flag("data-dir", "Directory to load test fixtures from").Required().Short('d').String()
		apiCommand                = app.Command(commands.API, "For running sotah-server.")
		syncItemsCommand          = app.Command(commands.SyncItems, "For syncing items in gcloud storage to local disk.")
		liveAuctionsCommand       = app.Command(commands.LiveAuctions, "For in-memory storage of current auctions.")
		pricelistHistoriesCommand = app.Command(commands.PricelistHistories, "For on-disk storage of pricelist histories.")
		pruneStoreCommand         = app.Command(commands.PruneStore, "For pruning gcloud store of non-primary region auctions.")
		reformHistoryCommand      = app.Command(commands.ReformHistory, "For testing history reformation.")
	)
	cmd := kingpin.MustParse(app.Parse(os.Args[1:]))

	logVerbosity, err := logrus.ParseLevel(*verbosity)
	if err != nil {
		logging.WithField("error", err.Error()).Fatal("Could not parse log level")

		return
	}
	logging.SetLevel(logVerbosity)

	// optionally adding logstash hook
	var logstashConn net.Conn
	hasLogstashParams := logstashHost != nil && logstashPort != nil && *logstashHost != "" && *logstashPort != 0
	if hasLogstashParams {
		logging.Info("Connecting to logstash")

		if logstashConn, err = net.Dial("tcp", fmt.Sprintf("%s:%d", *logstashHost, *logstashPort)); err != nil {
			logging.WithFields(logrus.Fields{
				"error": err.Error(),
				"host":  *logstashHost,
				"port":  *logstashPort,
			}).Fatal("Could not dial logstash host")

			return
		}

		logging.AddHook(logrusstash.New(logstashConn, logrusstash.DefaultFormatter(logrus.Fields{})))
	}
	logging.Info("Starting")

	// loading the config file
	c, err := newConfigFromFilepath(*configFilepath)
	if err != nil {
		logging.WithFields(logrus.Fields{
			"error":    err.Error(),
			"filepath": *configFilepath,
		}).Fatal("Could not fetch config")

		return
	}

	// optionally overriding api key in config
	if len(*apiKey) > 0 {
		logging.WithField("api-key", *apiKey).Info("Overriding api key found in config")

		c.APIKey = *apiKey
	}

	// optionally overriding cache-dir in config
	if len(*cacheDir) > 0 {
		logging.WithField("cache-dir", *cacheDir).Info("Overriding cache-dir found in config")

		c.CacheDir = *cacheDir
	}

	// validating the cache dir
	if c.CacheDir == "" {
		logging.Fatal("Cache-dir cannot be blank")

		return
	}

	// connecting the messenger
	mess, err := newMessenger(*natsHost, *natsPort)
	if err != nil {
		logging.WithFields(logrus.Fields{
			"error": err.Error(),
			"host":  *natsHost,
			"port":  *natsPort,
		}).Fatal("Could not connect messenger")

		return
	}

	// connecting storage
	stor := store{}
	if c.UseGCloudStorage {
		stor, err = newStore(*projectID)
		if err != nil {
			logging.WithFields(logrus.Fields{
				"error":     err.Error(),
				"projectId": *projectID,
			}).Fatal("Could not connect store")

			return
		}
	}

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
		reformHistoryCommand.FullCommand(): func() error {
			return reformHistory(c, mess, stor)
		},
	}
	cmdFunc, ok := cMap[cmd]
	if !ok {
		logging.WithField("command", cmd).Fatal("Invalid command")

		return
	}

	if hasLogstashParams {
		logging.ResetLogger(
			logVerbosity,
			logrusstash.New(logstashConn, logrusstash.DefaultFormatter(logrus.Fields{"command": cmd})),
		)
	}

	if err := cmdFunc(); err != nil {
		logging.WithFields(logrus.Fields{
			"error":   err.Error,
			"command": cmd,
		}).Fatal("Failed to execute command")
	}
}
