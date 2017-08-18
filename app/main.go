package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	// parsing the command flags
	var (
		app            = kingpin.New("sotah-server", "A command-line Blizzard AH client.")
		natsHost       = app.Flag("nats_host", "NATS hostname").Default("localhost").OverrideDefaultFromEnvar("NATS_HOST").String()
		natsPort       = app.Flag("nats_port", "NATS port").Default("4222").OverrideDefaultFromEnvar("NATS_PORT").Int()
		configFilepath = app.Flag("config", "Relative path to config json").Required().String()
		apiKey         = app.Flag("api_key", "Blizzard Mashery API key").OverrideDefaultFromEnvar("API_KEY").String()
	)
	kingpin.Parse()

	// loading the config file
	c, err := newConfigFromFilepath(*configFilepath)
	if err != nil {
		log.Fatalf("Could not fetch config: %s\n", err.Error())

		return
	}

	// loading a resolver with the config
	res := newResolver(c)

	// optionally overriding api key in config
	if len(*apiKey) > 0 {
		c.APIKey = *apiKey
	}

	// connecting the messenger
	messenger, err := newMessenger(*natsHost, *natsPort)
	if err != nil {
		log.Fatalf("Could not connect messenger: %s\n", err.Error())

		return
	}

	// establishing a state and filling it with statuses
	sta := state{
		messenger: messenger,
		config:    c,
		statuses:  map[regionName]*status{},
	}
	for _, reg := range c.Regions {
		stat, err := newStatusFromHTTP(reg, res)
		if err != nil {
			log.Fatalf("Could not fetch statuses from http: %s\n", err.Error())

			return
		}

		sta.statuses[reg.Name] = stat
	}

	// listening for status requests
	stopChans := map[string]chan interface{}{
		"status":  make(chan interface{}),
		"regions": make(chan interface{}),
	}
	if err := sta.listenForStatus(stopChans["status"]); err != nil {
		log.Fatalf("Could not listen for status requests: %s\n", err.Error())

		return
	}
	if err := sta.listenForRegions(stopChans["regions"]); err != nil {
		log.Fatalf("Could not listen for regions requests: %s\n", err.Error())

		return
	}

	fmt.Printf("Running!\n")

	// catching SIGINT
	sigIn := make(chan os.Signal, 1)
	signal.Notify(sigIn, os.Interrupt)
	<-sigIn
	fmt.Printf("Caught SIGINT!\n")

	// stopping listeners
	for _, stop := range stopChans {
		stop <- struct{}{}
	}

	// exiting
	os.Exit(0)
}
