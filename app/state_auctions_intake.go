package main

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/ihsw/sotah-server/app/blizzard"

	"github.com/ihsw/sotah-server/app/subjects"
	nats "github.com/nats-io/go-nats"
	log "github.com/sirupsen/logrus"
)

func newAuctionsIntakeRequest(payload []byte) (auctionsIntakeRequest, error) {
	ar := &auctionsIntakeRequest{}
	err := json.Unmarshal(payload, &ar)
	if err != nil {
		return auctionsIntakeRequest{}, err
	}

	return *ar, nil
}

func (aiRequest auctionsIntakeRequest) resolve(sta state) (map[regionName]realms, error) {
	out := map[regionName]realms{}
	for rNAme, realmSlugs := range aiRequest.RegionRealmSlugs {
		statusValue, ok := sta.statuses[rNAme]
		if !ok {
			return nil, errors.New("Invalid region")
		}

		out[rNAme] = realms{}
		for _, rea := range statusValue.Realms {
			for _, inRealm := range realmSlugs {
				if rea.Slug != inRealm {
					continue
				}

				out[rNAme] = append(out[rNAme], rea)
			}
		}
	}

	return out, nil
}

type auctionsIntakeRequest struct {
	RegionRealmSlugs map[regionName][]blizzard.RealmSlug `json:"region_realmslugs"`
}

func (sta state) listenForAuctionsIntake(stop listenStopChan) error {
	// spinning up a worker for handling auctions-intake requests
	in := make(chan auctionsIntakeRequest, 10)
	go func() {
		for aiRequest := range in {
			regionRealms, err := aiRequest.resolve(sta)
			if err != nil {
				log.WithField("error", err.Error()).Info("Failed to resolve auctions-intake-request")

				continue
			}

			totalRealms := 0
			for rName, reas := range sta.statuses {
				totalRealms += len(reas.Realms.filterWithWhitelist(sta.resolver.config.Whitelist[rName]))
			}
			processedRealms := 0
			for _, reas := range regionRealms {
				processedRealms += len(reas)
			}

			startTime := time.Now()

			for _, reas := range regionRealms {
				loadedAuctions := reas.loadAuctionsFromCacheDir(sta.resolver.config)
				for job := range loadedAuctions {
					if job.err != nil {
						log.WithFields(log.Fields{
							"region": job.realm.region.Name,
							"realm":  job.realm.Slug,
							"error":  err.Error(),
						}).Info("Failed to load auctions from filecache")

						continue
					}

					sta.auctions[job.realm.region.Name][job.realm.Slug] = newMiniAuctionListFromBlizzardAuctions(job.auctions.Auctions)
				}
			}

			log.WithFields(log.Fields{
				"total_realms":     totalRealms,
				"processed_realms": processedRealms,
			}).Info("Processed all realms")
			sta.messenger.publishMetric(telegrafMetrics{
				"intake_duration": int64(time.Now().Unix() - startTime.Unix()),
				"intake_count":    int64(processedRealms),
			})
		}
	}()

	// starting up a listener for auctions-intake
	err := sta.messenger.subscribe(subjects.AuctionsIntake, stop, func(natsMsg nats.Msg) {
		// resolving the request
		aiRequest, err := newAuctionsIntakeRequest(natsMsg.Data)
		if err != nil {
			log.Info("Failed to parse auctions-intake-request")

			return
		}

		in <- aiRequest
	})
	if err != nil {
		return err
	}

	return nil
}
