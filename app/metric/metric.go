package metric

import (
	"fmt"
	"net/url"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/logging"
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

	logging.WithFields(fields).Info(defaultMessage)
}

// BlizzardAPIIngressMetrics - encapsulation of blizzard api metrics
type BlizzardAPIIngressMetrics struct {
	ByteCount int
	Duration  time.Duration
}

func (b BlizzardAPIIngressMetrics) toFields() logrus.Fields {
	durationInMicroseconds := int64(b.Duration) / 1000
	return logrus.Fields{"byte_count": b.ByteCount, "duration": durationInMicroseconds}
}

// ReportBlizzardAPIIngress - for knowing how much network ingress is happening via blizzard api
func ReportBlizzardAPIIngress(uri string, m BlizzardAPIIngressMetrics) error {
	// obfuscating the access token from the uri before logging
	uri, err := func() (string, error) {
		u, err := url.Parse(uri)
		if err != nil {
			return "", err
		}

		q := u.Query()
		q.Set("access_token", "xxx")
		u.RawQuery = q.Encode()

		return u.String(), nil
	}()
	if err != nil {
		return err
	}

	// converting to logrus fields
	fields := m.toFields()
	fields["uri"] = uri

	// reporting
	report(blizzardAPIIngress, fields)

	return nil
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
	fields[fmt.Sprintf("%s_duration_length", kind)] = length

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
