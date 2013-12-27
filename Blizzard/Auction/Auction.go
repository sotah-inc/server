package Auction

import (
	"github.com/ihsw/go-download/Entity"
)

/*
	misc
*/
type Response struct {
	Files []File
}

type File struct {
	LastModified uint64
	Url          string
}

const URL_FORMAT = "http://%s/api/wow/auction/data/%s"

/*
	chan structs
*/
type Result struct {
	Response Response
	Length   int64
	Realm    Entity.Realm
	Error    error
}
