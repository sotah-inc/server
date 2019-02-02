module github.com/sotah-inc/server/app/pkg/metric

replace github.com/sotah-inc/server/app/pkg/logging => ../logging

require (
	github.com/sirupsen/logrus v1.3.0
	github.com/sotah-inc/server/app/pkg/logging v0.0.0-20190202132759-df45093b39b7
)
