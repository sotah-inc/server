package metric

import (
	"github.com/sirupsen/logrus"
)

// Name - typehint for these enums
type Name string

/*
Names - types of names
*/
const (
	BlizzardAPIIngress Name = "blizzard_api_ingress"
)

// Report - logs metrics
func Report(name Name, fields logrus.Fields, message string) {
	fields["metric"] = name

	logrus.WithFields(fields).Info(message)
}
