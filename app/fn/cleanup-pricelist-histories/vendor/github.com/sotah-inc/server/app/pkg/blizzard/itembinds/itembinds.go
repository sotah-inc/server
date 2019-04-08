package itembinds

// ItemBind - typehint for these enums
type ItemBind int

/*
ItemBinds - types of item-binds
*/
const (
	None ItemBind = iota
	BindOnPickup
	BindOnEquip
)
