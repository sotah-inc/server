package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ihsw/sotah-server/app/logging"
	"github.com/ihsw/sotah-server/app/util"
	"github.com/sirupsen/logrus"
)

func normalizeTargetDate(targetDate time.Time) time.Time {
	nearestWeekStartOffset := targetDate.Second() + targetDate.Minute()*60 + targetDate.Hour()*60*60
	return time.Unix(targetDate.Unix()-int64(nearestWeekStartOffset), 0)
}

func databaseRetentionLimit() time.Time {
	return time.Now().Add(-1 * time.Hour * 24 * 15)
}

type databasePathPair struct {
	fullPath   string
	targetTime time.Time
}

func databasePaths(databaseDir string) ([]databasePathPair, error) {
	out := []databasePathPair{}

	databaseFilepaths, err := ioutil.ReadDir(databaseDir)
	if err != nil {
		logging.WithFields(logrus.Fields{
			"error": err.Error(),
			"dir":   databaseDir,
		}).Error("Failed to read database dir")

		return []databasePathPair{}, err
	}

	for _, fPath := range databaseFilepaths {
		if fPath.Name() == "live-auctions.db" {
			continue
		}

		parts := strings.Split(fPath.Name(), ".")
		targetTimeUnix, err := strconv.Atoi(parts[0])
		if err != nil {
			logging.WithFields(logrus.Fields{
				"error":    err.Error(),
				"dir":      databaseDir,
				"pathname": fPath.Name(),
			}).Error("Failed to parse database filepath")

			return []databasePathPair{}, err
		}

		targetTime := time.Unix(int64(targetTimeUnix), 0)

		fullPath, err := filepath.Abs(fmt.Sprintf("%s/%s", databaseDir, fPath.Name()))
		if err != nil {
			logging.WithFields(logrus.Fields{
				"error":    err.Error(),
				"dir":      databaseDir,
				"pathname": fPath.Name(),
			}).Error("Failed to resolve full path of database file")

			return []databasePathPair{}, err
		}

		out = append(out, databasePathPair{fullPath, targetTime})
	}

	return out, nil
}

func newPriceListHistoryFromBytes(data []byte) (priceListHistory, error) {
	gzipDecoded, err := util.GzipDecode(data)
	if err != nil {
		return priceListHistory{}, err
	}

	out := priceListHistory{}
	if err := json.Unmarshal(gzipDecoded, &out); err != nil {
		return priceListHistory{}, err
	}

	return out, nil
}

type priceListHistory map[unixTimestamp]prices

func (plHistory priceListHistory) encodeForPersistence() ([]byte, error) {
	jsonEncoded, err := json.Marshal(plHistory)
	if err != nil {
		return []byte{}, err
	}

	gzipEncoded, err := util.GzipEncode(jsonEncoded)
	if err != nil {
		return []byte{}, err
	}

	return gzipEncoded, nil
}
