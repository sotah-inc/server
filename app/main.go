package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
)

func main() {
	// parsing the command flags
	configFilepath := flag.String("config", "", "Relative path to config json")
	natsHost := flag.String("nats_host", "", "Hostname of nats server")
	natsPort := flag.Int("nats_port", 0, "Port number of nats server")
	flag.Parse()

	// loading the config file
	c, err := newConfigFromFilepath(*configFilepath)
	if err != nil {
		log.Fatalf("Could not fetch config: %s\n", err.Error())

		return
	}
	res := newResolver(c.APIKey)

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
