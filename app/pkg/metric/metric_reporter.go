package metric

import (
	"encoding/json"
	"fmt"

	"github.com/sotah-inc/server/app/pkg/metric/kinds"

	"github.com/sotah-inc/server/app/pkg/messenger"
	"github.com/sotah-inc/server/app/pkg/messenger/subjects"
)

func NewReporter(mess messenger.Messenger) Reporter {
	return Reporter{mess}
}

type Reporter struct {
	Messenger messenger.Messenger
}

type Metrics map[string]int

func (re Reporter) Report(m Metrics) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}

	return re.Messenger.Publish(subjects.AppMetrics, data)
}

func (re Reporter) ReportWithPrefix(m Metrics, prefix kinds.Kind) error {
	next := Metrics{}

	for k, v := range m {
		next[fmt.Sprintf("%s_%s", prefix, k)] = v
	}

	return re.Report(next)
}
