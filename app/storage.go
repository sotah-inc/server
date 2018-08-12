package main

func newStorage(projectID string) storage {
	return storage{projectID}
}

type storage struct {
	projectID string
}
