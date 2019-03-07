package resolver

import (
	"github.com/sirupsen/logrus"
	"github.com/sotah-inc/server/app/pkg/blizzard"
	"github.com/sotah-inc/server/app/pkg/util"
)

func (r Resolver) GetItemIconData(uri string) ([]byte, error) {
	resp, err := r.Download(uri, false)
	if err != nil {
		return []byte{}, err
	}

	return resp.Body, nil
}

type GetItemIconsJob struct {
	Err      error
	IconName string
	Data     []byte
}

func (job GetItemIconsJob) ToLogrusFields() logrus.Fields {
	return logrus.Fields{
		"error":     job.Err.Error(),
		"icon-name": job.IconName,
	}
}

func (r Resolver) GetItemIcons(iconNames []string) chan GetItemIconsJob {
	// establishing channels
	out := make(chan GetItemIconsJob)
	in := make(chan string)

	// spinning up the workers for fetching items
	worker := func() {
		for iconName := range in {
			iconData, err := r.GetItemIconData(blizzard.DefaultGetItemIconURL(iconName))
			out <- GetItemIconsJob{err, iconName, iconData}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(8, worker, postWork)

	// queueing up the items
	go func() {
		for _, iconName := range iconNames {
			in <- iconName
		}

		close(in)
	}()

	return out
}
