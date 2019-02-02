module github.com/sotah-inc/server/app/pkg/blizzard

replace github.com/sotah-inc/server/app/pkg/utiltest => ../utiltest

replace github.com/sotah-inc/server/app/pkg/util => ../util

replace github.com/sotah-inc/server/app/pkg/logging => ../logging

replace github.com/sotah-inc/server/app/pkg/metric => ../metric

require (
	github.com/sirupsen/logrus v1.3.0
	github.com/sotah-inc/server v0.0.0-20190202132759-df45093b39b7
	github.com/sotah-inc/server/app/pkg/logging v0.0.0-20190202132759-df45093b39b7
	github.com/sotah-inc/server/app/pkg/util v0.0.0-20190202132759-df45093b39b7
	github.com/sotah-inc/server/app/pkg/utiltest v0.0.0-20190202132759-df45093b39b7
	github.com/stretchr/testify v1.3.0
)
