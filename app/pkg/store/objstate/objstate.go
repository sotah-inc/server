package objstate

// ObjState - typehint for these enums
type ObjState string

/*
ObjState - types of gcloud storage state
*/
const (
	Queued    ObjState = "queued-%s"
	Processed ObjState = "processed-2018-09-26-1245"
)
