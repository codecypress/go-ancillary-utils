package cypressutils

type EnumInterface interface {
	name() string
	ordinal() int
	values() *[]string
}

type Enum struct {
	EnumInterface
}
