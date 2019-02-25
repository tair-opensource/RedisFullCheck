package main

type KeyTypeIndex int

type KeyType struct {
	name            string // type
	index           KeyTypeIndex
	fetchLenCommand string
}

const (
	StringTypeIndex KeyTypeIndex = iota
	HashTypeIndex
	ListTypeIndex
	SetTypeIndex
	ZsetTypeIndex
	NoneTypeIndex
	EndKeyTypeIndex
)

func (p KeyType) String() string {
	return p.name
}

var StringType = &KeyType{
	name:            "string",
	index:           StringTypeIndex,
	fetchLenCommand: "strlen",
}

var HashType = &KeyType{
	name:            "hash",
	index:           HashTypeIndex,
	fetchLenCommand: "hlen",
}

var ListType = &KeyType{
	name:            "list",
	index:           ListTypeIndex,
	fetchLenCommand: "llen",
}

var SetType = &KeyType{
	name:            "set",
	index:           SetTypeIndex,
	fetchLenCommand: "scard",
}

var ZsetType = &KeyType{
	name:            "zset",
	index:           ZsetTypeIndex,
	fetchLenCommand: "zcard",
}

var NoneType = &KeyType{
	name:            "none",
	index:           NoneTypeIndex,
	fetchLenCommand: "strlen",
}

var EndKeyType = &KeyType{
	name:            "unkown",
	index:           EndKeyTypeIndex,
	fetchLenCommand: "unkown",
}

func NewKeyType(a string) *KeyType {
	switch a {
	case "string":
		return StringType
	case "hash":
		return HashType
	case "list":
		return ListType
	case "set":
		return SetType
	case "zset":
		return ZsetType
	case "none":
		return NoneType
	default:
		return EndKeyType
	}
}

func (p KeyTypeIndex) String() string {
	switch p {
	case StringTypeIndex:
		return "string"
	case HashTypeIndex:
		return "hash"
	case ListTypeIndex:
		return "list"
	case SetTypeIndex:
		return "set"
	case ZsetTypeIndex:
		return "zset"
	case NoneTypeIndex:
		return "none"
	default:
		return "none"
	}
}

type Field struct {
	field        []byte
	conflictType ConflictType
}

type Attribute struct {
	itemcount int64
}

type Key struct {
	key          []byte
	db           int32
	tp           *KeyType
	conflictType ConflictType
	sourceAttr   Attribute
	targetAttr   Attribute

	field []Field
}

type ConflictType int

const (
	TypeConflict ConflictType = iota
	ValueConflict
	LackSourceConflict
	LackTargetConflict
	NoneConflict
	EndConflict
)

func (p ConflictType) String() string {
	switch p {
	case TypeConflict:
		return "type"
	case ValueConflict:
		return "value"
	case LackSourceConflict:
		return "lack_source"
	case LackTargetConflict:
		return "lack_target"
	case NoneConflict:
		return "equal"
	default:
		return "unkown_conflict"
	}
}

func NewConflictType(a string) ConflictType {
	switch a {
	case "type":
		return TypeConflict
	case "value":
		return ValueConflict
	case "lack_source":
		return LackSourceConflict
	case "lack_target":
		return LackTargetConflict
	case "equal":
		return NoneConflict
	default:
		return EndConflict
	}
}
