package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ihsw/sotah-server/app/util"
	log "github.com/sirupsen/logrus"
)

const itemIconURLFormat = "https://render-us.worldofwarcraft.com/icons/56/%s.jpg"

func defaultGetItemIconURL(name string) string {
	return fmt.Sprintf(itemIconURLFormat, name)
}

type getItemIconURLFunc func(string) string

type syncItemIconsJob struct {
	err  error
	icon string
}

func syncItemIcons(icons []string, res resolver) chan syncItemIconsJob {
	// establishing channels
	out := make(chan syncItemIconsJob)
	in := make(chan string)

	// spinning up the workers for fetching items
	worker := func() {
		for iconName := range in {
			err := syncItemIcon(iconName, res)
			out <- syncItemIconsJob{err: err, icon: iconName}
		}
	}
	postWork := func() {
		close(out)
	}
	util.Work(8, worker, postWork)

	// queueing up the realms
	go func() {
		for _, iconName := range icons {
			in <- iconName
		}

		close(in)
	}()

	return out
}

func syncItemIcon(name string, res resolver) error {
	if res.config == nil {
		return errors.New("Config cannot be nil")
	}

	if res.config.UseCacheDir == false {
		_, err := res.get(res.getItemIconURL(name))
		return err
	}

	if res.config.CacheDir == "" {
		return errors.New("Cache dir cannot be blank")
	}

	itemIconFilepath, err := filepath.Abs(
		fmt.Sprintf("%s/item-icons/%s.jpg", res.config.CacheDir, name),
	)
	if err != nil {
		return err
	}

	if _, err := os.Stat(itemIconFilepath); err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		log.WithField("name", name).Info("Fetching item-icon")

		body, err := res.get(res.getItemIconURL(name))
		if err != nil {
			return err
		}

		if err := util.WriteFile(itemIconFilepath, body); err != nil {
			return err
		}
	}

	return nil
}
