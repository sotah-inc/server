package bus

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/bus/codes"
	"github.com/sotah-inc/server/app/pkg/sotah"
	"github.com/sotah-inc/server/app/pkg/state/subjects"
	"github.com/sotah-inc/server/app/pkg/util"
)

type StatusRequest struct {
	RegionName blizzard.RegionName `json:"region_name"`
}

func (c Client) NewStatus(reg sotah.Region) (sotah.Status, error) {
	lm := StatusRequest{RegionName: reg.Name}
	encodedMessage, err := json.Marshal(lm)
	if err != nil {
		return sotah.Status{}, err
	}

	msg, err := c.RequestFromTopic(string(subjects.Status), string(encodedMessage), 5*time.Second)
	if err != nil {
		return sotah.Status{}, err
	}

	if msg.Code != codes.Ok {
		return sotah.Status{}, errors.New(msg.Err)
	}

	stat, err := blizzard.NewStatus([]byte(msg.Data))
	if err != nil {
		return sotah.Status{}, err
	}

	return sotah.NewStatus(reg, stat), nil
}

type LoadStatusesJob struct {
	Err    error
	Region sotah.Region
	Status sotah.Status
}

func (c Client) LoadStatuses(regions sotah.RegionList) chan LoadStatusesJob {
	// establishing channels
	in := make(chan sotah.Region)
	out := make(chan LoadStatusesJob)

	// spinning up the workers
	worker := func() {
		for region := range in {
			status, err := c.NewStatus(region)
			if err != nil {
				out <- LoadStatusesJob{
					Err:    err,
					Region: region,
					Status: sotah.Status{},
				}

				continue
			}

			out <- LoadStatusesJob{
				Err:    nil,
				Region: region,
				Status: status,
			}
		}
	}
	postWork := func() {
		return
	}
	util.Work(4, worker, postWork)

	// queueing up the regions
	go func() {
		for _, region := range regions {
			in <- region
		}

		close(in)
	}()

	return out
}
