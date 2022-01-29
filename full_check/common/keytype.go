package common

type KeyTypeIndex int

const (
	StringTypeIndex KeyTypeIndex = iota
	HashTypeIndex
	ListTypeIndex
	SetTypeIndex
	ZsetTypeIndex
	StreamTypeIndex
	NoneTypeIndex
	EndKeyTypeIndex
)

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
	case StreamTypeIndex:
		return "stream"
	case NoneTypeIndex:
		return "none"
	default:
		return "none"
	}
}

type KeyType struct {
	Name            string // type
	Index           KeyTypeIndex
	FetchLenCommand string
}

func (p KeyType) String() string {
	return p.Name
}

var StringKeyType = &KeyType{
	Name:            "string",
	Index:           StringTypeIndex,
	FetchLenCommand: "strlen",
}

var HashKeyType = &KeyType{
	Name:            "hash",
	Index:           HashTypeIndex,
	FetchLenCommand: "hlen",
}

var ListKeyType = &KeyType{
	Name:            "list",
	Index:           ListTypeIndex,
	FetchLenCommand: "llen",
}

var SetKeyType = &KeyType{
	Name:            "set",
	Index:           SetTypeIndex,
	FetchLenCommand: "scard",
}

var ZsetKeyType = &KeyType{
	Name:            "zset",
	Index:           ZsetTypeIndex,
	FetchLenCommand: "zcard",
}

var StreamKeyType = &KeyType{
	Name:            "stream",
	Index:           StreamTypeIndex,
	FetchLenCommand: "xlen",
}

var NoneKeyType = &KeyType{
	Name:            "none",
	Index:           NoneTypeIndex,
	FetchLenCommand: "strlen",
}

var EndKeyType = &KeyType{
	Name:            "unknown",
	Index:           EndKeyTypeIndex,
	FetchLenCommand: "unknown",
}

func NewKeyType(a string) *KeyType {
	switch a {
	case "string":
		return StringKeyType
	case "hash":
		return HashKeyType
	case "list":
		return ListKeyType
	case "set":
		return SetKeyType
	case "zset":
		return ZsetKeyType
	case "stream":
		return StreamKeyType
	case "none":
		return NoneKeyType
	default:
		return EndKeyType
	}
}

type Field struct {
	Field        []byte
	ConflictType ConflictType
}

type Attribute struct {
	ItemCount int64 // the length of value
}

type Key struct {
	Key          []byte
	Db           int32
	Tp           *KeyType
	ConflictType ConflictType
	SourceAttr   Attribute
	TargetAttr   Attribute

	Field []Field
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
		return "unknown_conflict"
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
