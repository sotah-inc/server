package metric

import (
	"github.com/sirupsen/logrus"
)

// Name - typehint for these enums
type Name string

/*
Names - names of metrics
*/
const (
	BlizzardAPIIngress Name = "blizzard_api_ingress"
)

func report(name Name, fields logrus.Fields, message string) {
	fields["metric"] = name

	logrus.WithFields(fields).Info(message)
}

// ReportBlizzardAPIIngress - metric report func for logging uncompressed byte ingress (for the big numbers)
func ReportBlizzardAPIIngress(message string, byteCount int) {
	report(BlizzardAPIIngress, logrus.Fields{"byte_count": byteCount}, message)
}
