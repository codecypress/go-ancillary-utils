package cypressutils

import (
	cErrors "github.com/pkg/errors"
	"strings"
)

type FilterPredicate struct {
	predicateClause string
}

func NewFilterPredicate(predicateClause ...string) *FilterPredicate {
	pred := ""
	if predicateClause != nil {
		pred = predicateClause[0]
	}

	return &FilterPredicate{predicateClause: pred}
}

func NewFilterPredicateWithInit(predicateClause string) *FilterPredicate {
	return &FilterPredicate{predicateClause: predicateClause}
}

func (predicate *FilterPredicate) OpenPredicate() *FilterPredicate {
	predicate.predicateClause += "("
	return predicate
}

func (predicate *FilterPredicate) ClosePredicate() *FilterPredicate {
	predicate.predicateClause += ")"
	return predicate
}

func (predicate *FilterPredicate) Or() *FilterPredicate {
	predicate.predicateClause += " OR "
	return predicate
}

func (predicate *FilterPredicate) And() *FilterPredicate {
	predicate.predicateClause += " AND "
	return predicate
}

func (predicate *FilterPredicate) Not() *FilterPredicate {
	predicate.predicateClause += " NOT "
	return predicate
}

func (predicate *FilterPredicate) IsNotNull(column string) *FilterPredicate {
	predicate.predicateClause += column + " IS NOT NULL "
	return predicate
}

func (predicate *FilterPredicate) IsNull(column string) *FilterPredicate {
	predicate.predicateClause += column + " IS NULL "
	return predicate
}

func (predicate *FilterPredicate) CustomFilter(filter string) *FilterPredicate {
	predicate.predicateClause += " " + filter + " "
	return predicate
}

func (predicate *FilterPredicate) Like(column, namedVariable string) *FilterPredicate {
	if column == "" {
		err := cErrors.New("LIKE: Column name is empty")
		ThrowException(err)
		return predicate
	}

	validateColumnArgument(namedVariable, "LIKE: ")

	predicate.predicateClause += " " + column + " LIKE " + namedVariable + " "
	return predicate
}

func (predicate *FilterPredicate) ILike(column, namedVariable string) *FilterPredicate {
	if column == "" {
		err := cErrors.New("ILIKE: Column name is empty")
		ThrowException(err)
		return predicate
	}

	validateColumnArgument(namedVariable, "ILIKE: ")

	predicate.predicateClause += " " + column + " ILIKE " + namedVariable + " "
	return predicate
}

func (predicate *FilterPredicate) In(column, query string) *FilterPredicate {
	if column == "" {
		err := cErrors.New("IN: Column name is empty")
		ThrowException(err)
		return predicate
	}

	if query == "" {
		err := cErrors.New("IN: Phrase is empty")
		ThrowException(err)
		return predicate
	}
	predicate.predicateClause += " " + column + " IN (" + query + ") "

	return predicate
}

func (predicate *FilterPredicate) NotIn(column, query string) *FilterPredicate {
	if column == "" {
		panic("NOT IN: Column name is empty")
	}

	if query == "" {
		panic("NOT IN: Phrase is empty")
	}
	predicate.predicateClause += " " + column + " NOT IN (" + query + ") "

	return predicate
}

func (predicate *FilterPredicate) CopyFilterFrom(filterPredicate *FilterPredicate) *FilterPredicate {
	if filterPredicate != nil {
		predicate.predicateClause += " " + filterPredicate.predicateClause
	}
	return predicate
}

func (predicate *FilterPredicate) Any(subQuery string) *FilterPredicate {
	predicate.predicateClause += " ANY (" + subQuery + ") "
	return predicate
}

func (predicate *FilterPredicate) All(subQuery string) *FilterPredicate {
	predicate.predicateClause += " ALL (" + subQuery + ") "
	return predicate
}

func (predicate *FilterPredicate) Exists(subQuery string) *FilterPredicate {
	predicate.predicateClause += " EXISTS (" + subQuery + ") "
	return predicate
}

func (predicate *FilterPredicate) Some(subQuery string) *FilterPredicate {
	predicate.predicateClause += " SOME (" + subQuery + ") "
	return predicate
}

func (predicate *FilterPredicate) Between(column, namedVariable1, namedVariable2 string) *FilterPredicate {
	if column == "" {
		panic("LESS THAN: Column name is empty")
	}

	validateColumnArgument(namedVariable1, "BETWEEN: ")
	validateColumnArgument(namedVariable2, "BETWEEN: ")

	predicate.predicateClause += " " + column + " BETWEEN " + namedVariable1 + " AND " + namedVariable2 + " "
	return predicate
}

func (predicate *FilterPredicate) LessThan(column, namedVariable string) *FilterPredicate {
	if column == "" {
		panic("LESS THAN: Column name is empty")
	}

	validateColumnArgument(namedVariable, "LESS THAN: ")

	predicate.predicateClause += " " + column + " < " + namedVariable + " "
	return predicate
}

func (predicate *FilterPredicate) LessThanSubQuery(column, subQuery string) *FilterPredicate {
	if column == "" {
		panic("LESS THAN: Column name is empty")
	}

	if subQuery == "" {
		panic("LESS THAN: Sub-query is empty")
	}

	predicate.predicateClause += " " + column + " < (" + subQuery + ") "
	return predicate
}

func (predicate *FilterPredicate) LessThanOrEqualTo(column, namedVariable string) *FilterPredicate {
	if column == "" {
		panic("LESS THAN OR EQUAL TO: Column name is empty")
	}

	validateColumnArgument(namedVariable, "LESS THAN OR EQUAL TO: ")

	predicate.predicateClause += " " + column + " <= " + namedVariable + " "
	return predicate
}

func (predicate *FilterPredicate) LessThanOrEqualToSubQuery(column, subQuery string) *FilterPredicate {
	if column == "" {
		panic("LESS THAN OR EQUAL TO: Column name is empty")
	}

	if subQuery == "" {
		panic("LESS THAN OR EQUAL TO: Sub-query is empty")
	}

	predicate.predicateClause += " " + column + " <= (" + subQuery + ") "
	return predicate
}

func (predicate *FilterPredicate) GreaterThan(column, namedVariable string) *FilterPredicate {
	if column == "" {
		panic("GREATER THAN: Column name is empty")
	}

	validateColumnArgument(namedVariable, "GREATER THAN: ")

	predicate.predicateClause += " " + column + " > " + namedVariable + " "
	return predicate
}

func (predicate *FilterPredicate) GreaterThanSubQuery(column, subQuery string) *FilterPredicate {
	if column == "" {
		panic("GREATER THAN: Column name is empty")
	}

	if subQuery == "" {
		panic("GREATER THAN: Sub-query is empty")
	}

	predicate.predicateClause += " " + column + " > (" + subQuery + ") "
	return predicate
}

func (predicate *FilterPredicate) GreaterThanOrEqualTo(column, namedVariable string) *FilterPredicate {
	if column == "" {
		panic("GREATER THAN OR EQUAL TO: Column name is empty")
	}

	validateColumnArgument(namedVariable, "GREATER THAN OR EQUAL TO: ")

	predicate.predicateClause += " " + column + " >= " + namedVariable + " "
	return predicate
}

func (predicate *FilterPredicate) GreaterThanOrEqualToSubQuery(column, subQuery string) *FilterPredicate {
	if column == "" {
		panic("GREATER THAN OR EQUAL TO: Column name is empty")
	}

	if subQuery == "" {
		panic("GREATER THAN OR EQUAL TO: Sub-query is empty")
	}

	predicate.predicateClause += " " + column + " >= (" + subQuery + ") "
	return predicate
}

func (predicate *FilterPredicate) EqualTo(column, namedVariable string) *FilterPredicate {
	if column == "" {
		panic("EQUAL TO: Column name is empty")
	}

	validateColumnArgument(namedVariable, "EQUAL TO: ")

	predicate.predicateClause += " " + column + " = " + namedVariable + " "
	return predicate
}

func (predicate *FilterPredicate) EqualToSubQuery(column, subQuery string) *FilterPredicate {
	if column == "" {
		panic("EQUAL TO: Column name is empty")
	}

	if subQuery == "" {
		panic("EQUAL TO: Sub-query is empty")
	}

	predicate.predicateClause += " " + column + " = (" + subQuery + ") "
	return predicate
}

func (predicate *FilterPredicate) NotEqualTo(column, namedVariable string) *FilterPredicate {
	if column == "" {
		panic("NOT EQUAL TO: Column name is empty")
	}

	validateColumnArgument(namedVariable, "NOT EQUAL TO: ")

	predicate.predicateClause += " " + column + " <> " + namedVariable + " "
	return predicate
}

func (predicate *FilterPredicate) NotEqualToSubQuery(column, subQuery string) *FilterPredicate {
	if column == "" {
		panic("NOT EQUAL TO: Column name is empty")
	}

	if subQuery == "" {
		panic("NOT EQUAL TO: Sub-query is empty")
	}

	predicate.predicateClause += " " + column + " <> (" + subQuery + ") "
	return predicate
}

func (predicate *FilterPredicate) Case() *FilterPredicate {
	predicate.predicateClause += "(CASE "
	return predicate
}

func (predicate *FilterPredicate) End(succeedingComma ...bool) *FilterPredicate {
	predicate.predicateClause += "END)"

	if succeedingComma != nil && succeedingComma[0] {
		predicate.predicateClause += "), "
	} else {
		predicate.predicateClause += ") "
	}
	return predicate
}

func (predicate *FilterPredicate) EndAs(alias string, succeedingComma ...bool) *FilterPredicate {
	predicate.predicateClause += "END AS " + alias

	if succeedingComma != nil && succeedingComma[0] {
		predicate.predicateClause += "), "
	} else {
		predicate.predicateClause += ") "
	}

	return predicate
}

func (predicate *FilterPredicate) When(whenClause string) *FilterPredicate {
	predicate.predicateClause += "WHEN " + whenClause + " "
	return predicate
}

func (predicate *FilterPredicate) Then(namedVariable string) *FilterPredicate {
	if !strings.HasPrefix(namedVariable, ":") {
		panic("THEN CLAUSE IN CASE: namedVariable must start with full colon[:]")
	}
	if len(namedVariable) < 2 {
		panic("THEN CLAUSE IN CASE: namedVariable is empty")
	}

	if strings.Count(namedVariable, ":") > 1 {
		panic("THEN CLAUSE IN CASE: namedVariable can only have one full colon[:]")
	}

	predicate.predicateClause += " THEN " + namedVariable + " "
	return predicate
}

func (predicate *FilterPredicate) Else(namedVariable ...string) *FilterPredicate {
	if namedVariable == nil {
		predicate.predicateClause += " ELSE "
		return predicate
	}

	if !strings.HasPrefix(namedVariable[0], ":") {
		panic("ELSE CLAUSE IN CASE: namedVariable must start with full colon[:]")
	}
	if len(namedVariable[0]) < 2 {
		panic("ELSE CLAUSE IN CASE: namedVariable is empty")
	}

	if strings.Count(namedVariable[0], ":") > 1 {
		panic("ELSE CLAUSE IN CASE: namedVariable can only have one full colon[:]")
	}

	predicate.predicateClause += " ELSE " + namedVariable[0] + " "
	return predicate
}

func (predicate *FilterPredicate) GetClause() string {
	return predicate.predicateClause
}

func validateColumnArgument(namedVariable, errorLocation string) {
	if len(namedVariable) < 2 {
		panic(errorLocation + "namedVariable is empty")
	}

	if !strings.HasPrefix(namedVariable, ":") {
		panic(errorLocation + "'" + namedVariable + "' named variable must start with full colon[:]")
	}

	if strings.Count(namedVariable, ":") > 1 {
		panic(errorLocation + "'" + namedVariable + "' can only have one full colon[:]")
	}
}
