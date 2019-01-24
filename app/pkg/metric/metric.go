package metric

import (
	"fmt"
	"net/url"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/logging"
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
	ByteCount          int
	ConnectionDuration time.Duration
	RequestDuration    time.Duration
}

func (b BlizzardAPIIngressMetrics) toFields() logrus.Fields {
	connDurationInMilliseconds := int64(b.ConnectionDuration) / 1000 / 1000
	reqDurationInMilliseconds := int64(b.RequestDuration) / 1000 / 1000

	return logrus.Fields{
		"byte_count":    b.ByteCount,
		"conn_duration": connDurationInMilliseconds,
		"req_duration":  reqDurationInMilliseconds,
	}
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

// DurationMetrics - required metrics for every duration entry
type DurationMetrics struct {
	Duration       time.Duration
	TotalRealms    int
	IncludedRealms int
	ExcludedRealms int
}

func (d DurationMetrics) toFields(kind durationKind) logrus.Fields {
	durationInSeconds := int64(d.Duration) / 1000 / 1000 / 1000
	return logrus.Fields{
		"duration_kind":                         kind,
		"duration_length":                       durationInSeconds,
		fmt.Sprintf("%s_duration_length", kind): durationInSeconds,
		"total_realms":                          d.TotalRealms,
		"included_realms":                       d.IncludedRealms,
		"excluded_realms":                       d.ExcludedRealms,
	}
}

// ReportDuration - for knowing how long things take
func ReportDuration(kind durationKind, metrics DurationMetrics, fields logrus.Fields) {
	out := metrics.toFields(kind)
	for k, v := range fields {
		out[k] = v
	}

	report(operationalDuration, out)
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
