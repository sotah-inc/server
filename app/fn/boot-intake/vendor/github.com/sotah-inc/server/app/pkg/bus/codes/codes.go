package codes

// Code - typehint for these enums
type Code int

/*
Codes - message response codes
*/
var (
	Ok                Code = 1
	Blank             Code
	GenericError      Code = -1
	MsgJSONParseError Code = -2
	NotFound          Code = -3
	UserError         Code = -4
	BlizzardError     Code = -5
)
