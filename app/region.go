package app

type region struct {
	name     string
	hostname string
}

func newRegion(cr configRegion) region {
	return region{name: cr.Name, hostname: cr.Hostname}
}

func (reg region) getStatus(res resolver) (*status, error) {
	return newStatus(reg.hostname, res)
}
