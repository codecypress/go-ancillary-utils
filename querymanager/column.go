package querymanager

type OrderBy uint
type Aggregates uint

const (
	NO_ORDER = iota
	ASC
	DESC
)

const (
	NO_AGGREGATE = iota
	DISTINCT
	COUNT
	MAX
	MIN
	AVG
	SUM
)

var orderByStrings = []string{
	"NO_ORDER",
	"ASC",
	"DESC",
}

func (orderBy OrderBy) name() string {
	return orderByStrings[orderBy]
}

func (orderBy OrderBy) ordinal() int {
	return int(orderBy)
}

func (orderBy OrderBy) values() *[]string {
	return &orderByStrings
}

var aggregates = []string{
	"NO_AGGREGATE",
	"DISTINCT",
	"COUNT",
	"MAX",
	"MIN",
	"AVG",
	"SUM",
}

func (aggregate Aggregates) name() string {
	return aggregates[aggregate]
}

func (aggregate Aggregates) ordinal() int {
	return int(aggregate)
}

func (aggregate Aggregates) values() *[]string {
	return &aggregates
}

type Column struct {
	ColumnName, ColumnAlias string
	Aggregate               Aggregates
}

func (column *Column) GetColumnName() string {
	return column.ColumnName
}

func (column *Column) GetColumnAlias() string {
	return column.ColumnAlias
}

func (column *Column) GetAggregate() Aggregates {
	return column.Aggregate
}
