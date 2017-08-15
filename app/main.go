package main

import (
	"flag"
	"log"
)

func main() {
	// parsing the command flags
	configFilepath := flag.String("config", "", "Relative path to config json")
	natsHost := flag.String("nats_host", "", "Hostname of nats server")
	natsPort := flag.Int("nats_port", 0, "Port number of nats server")
	flag.Parse()

	// loading the config file
	config, err := newConfigFromFilepath(*configFilepath)
	if err != nil {
		log.Fatalf("Could not fetch config: %s\n", err.Error())

		return
	}
	res := newResolver(config.APIKey)

	// connecting the messenger
	messenger, err := newMessenger(*natsHost, *natsPort)
	if err != nil {
		log.Fatalf("Could not connect messenger: %s\n", err.Error())

		return
	}

	// going over the list of regions and loading the status for each one
	for _, reg := range config.Regions {
		sta, err := newStatusFromHTTP(reg, res)
	}
}
