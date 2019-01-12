package metric

import (
	"github.com/sirupsen/logrus"
)

const defaultMessage = "welp"

type name string

const (
	blizzardAPIIngress  name = "blizzard_api_ingress"
	operationalDuration name = "operational_duration"
	intakeBuffer        name = "intake_buffer"
)

func report(n name, fields logrus.Fields) {
	fields["metric"] = n

	logrus.WithFields(fields).Info(defaultMessage)
}

// ReportBlizzardAPIIngress - for knowing how much network ingress is happening via blizzard api
func ReportBlizzardAPIIngress(uri string, byteCount int) {
	report(blizzardAPIIngress, logrus.Fields{"byte_count": byteCount, "uri": uri})
}

type durationKind string

/*
kinds of duration metrics
*/
const (
	CollectorDuration        durationKind = "collector_duration"
	AuctionsIntakeDuration   durationKind = "auctions_intake_duration"
	PricelistsIntakeDuration durationKind = "pricelists_intake_duration"
)

// ReportDuration - for knowing how long things take
func ReportDuration(kind durationKind, length int64, fields logrus.Fields) {
	fields["duration_kind"] = kind
	fields["duration_length"] = length

	report(operationalDuration, fields)
}

type intakeKind string

/*
kinds of intake buffer metrics
*/
const (
	LiveAuctionsIntake intakeKind = "live_auctions_intake"
	PricelistsIntake   intakeKind = "pricelists_intake"
)

// ReportIntakeBufferSize - for knowing whether intake buffers are full
func ReportIntakeBufferSize(kind intakeKind, size int) {
	report(intakeBuffer, logrus.Fields{"intake_kind": kind, "intake_buffer_size": size})
}
