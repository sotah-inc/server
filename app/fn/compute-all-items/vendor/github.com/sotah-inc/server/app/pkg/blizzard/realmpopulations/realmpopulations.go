package realmpopulations

// RealmPopulation is a string enum of a realm-population kind
type RealmPopulation string

/*
RealmPopulations - kinds of realm-population
*/
const (
	Unknown RealmPopulation = "n/a"
	High    RealmPopulation = "high"
	Medium  RealmPopulation = "medium"
	Low     RealmPopulation = "low"
)
