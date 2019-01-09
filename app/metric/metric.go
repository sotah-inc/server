package metric

import (
	"github.com/sirupsen/logrus"
)

type name string

const (
	blizzardAPIIngress name = "blizzard_api_ingress"
)

func report(n name, fields logrus.Fields, message string) {
	fields["metric"] = n

	logrus.WithFields(fields).Info(message)
}

// ReportBlizzardAPIIngress - for knowing how much network ingress is happening via blizzard api
func ReportBlizzardAPIIngress(uri string, byteCount int) {
	report(blizzardAPIIngress, logrus.Fields{"byte_count": byteCount, "uri": uri}, "welp")
}
