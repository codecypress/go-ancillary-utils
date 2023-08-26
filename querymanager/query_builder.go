package querymanager

import (
	"bytes"
	"fmt"
	"github.com/codecypress/go-ancillary-utils/cypressutils"
	"github.com/codecypress/go-ancillary-utils/exceptions"
	cErrors "github.com/pkg/errors"
	"regexp"
	"strings"
)

type QueryBuilder struct {
	query, joinStatement, whereClause, tableName, primaryKeyColumn   string
	primaryKeyColumns, fetchedColumns, insertColumns, insertedValues []string
	fetchColumnsList                                                 []*Column
	Err                                                              error
}

/*--------------------------------START OF INSERT QUERIES---------------------------*/

func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{
		query:             "",
		joinStatement:     "",
		whereClause:       "",
		tableName:         "",
		primaryKeyColumn:  "",
		primaryKeyColumns: []string{},
		fetchedColumns:    []string{"*"},
		insertedValues:    []string{""},
		fetchColumnsList:  []*Column{},
		Err:               nil,
	}
}

func (builder *QueryBuilder) Insert() *QueryBuilder {
	builder.query += "INSERT INTO "
	return builder
}

func (builder *QueryBuilder) Prepend(prependStr string) *QueryBuilder {
	builder.query = prependStr + builder.query + " "
	return builder
}

func (builder *QueryBuilder) Append(appendStr string) *QueryBuilder {
	builder.query += appendStr + " "
	return builder
}

func (builder *QueryBuilder) Into(tableName string) *QueryBuilder {

	err := validateTableName(tableName, "INSERT: Table name is empty")
	if err != nil {
		return builder
	}
	builder.tableName = tableName
	builder.query += tableName + " "
	return builder
}

func (builder *QueryBuilder) Columns(columns []string) *QueryBuilder {
	builder.insertColumns = columns
	builder.query += concatenateColumnNames(columns) + " "
	return builder
}

func (builder *QueryBuilder) Values(namedVariables []string) *QueryBuilder {
	if namedVariables == nil || len(namedVariables) < 1 {
		err := cErrors.New("INSERT: No insert values provided")
		exceptions.ThrowException(err)
		builder.Err = err
		return builder
	}

	for _, value := range namedVariables {
		if !strings.HasPrefix(value, ":") && !strings.HasPrefix(value, "@") {
			err := cErrors.New("INSERT: '" + value + "' named variable must start with full colon[:]")
			exceptions.ThrowException(err)
			builder.Err = err
			return builder
		}
	}

	if builder.insertColumns != nil {
		if len(builder.insertColumns) != len(namedVariables) {
			err := cErrors.New("INSERT: Number of columns and values do not match")
			exceptions.ThrowException(err)
			builder.Err = err
			return builder
		}
	}

	builder.query += " VALUES " + concatenateColumnNames(namedVariables)
	return builder
}

func (builder *QueryBuilder) ValuesConcatenated(valuesString string) *QueryBuilder {
	if valuesString == "" {
		err := cErrors.New("INSERT: No insert values provided")
		exceptions.ThrowException(err)
		builder.Err = err
		return builder
	}

	builder.query += " VALUES " + valuesString
	return builder
}

func (builder *QueryBuilder) OnDuplicateKey(columns []string) *QueryBuilder {
	if columns == nil || len(columns) == 0 {
		err := cErrors.New("INSERT: No on duplicate values provided")
		exceptions.ThrowException(err)
		builder.Err = err
		return builder
	}

	builder.query += " ON DUPLICATE KEY UPDATE "
	shouldAddComma := false

	for index := range columns {
		column := columns[index]
		if shouldAddComma {
			builder.query += ", "
		} else {
			shouldAddComma = true
		}

		builder.query += column + " = :" + column
	}

	return builder
}

func (builder *QueryBuilder) ValuesFromSelect(selectSubQuery string) *QueryBuilder {
	builder.query += "(" + selectSubQuery + ") "
	return builder
}

/*   public Query insertValuesTermination() throws QueryBuilderException {
     if (insertedValues[0].isEmpty()) throw new QueryBuilderException("INSERT: Insert values missing in query: " + query);
     query += " VALUES " + insertedValues[0];
     return this;
 }*/

/*--------------------------------START OF UPDATE QUERIES-------------------------------------*/

func (builder *QueryBuilder) Update(tableName string) *QueryBuilder {
	err := validateTableName(tableName, "UPDATE: Table name is empty")
	if err != nil {
		return builder
	}

	builder.tableName = tableName
	builder.query += "UPDATE " + tableName + " SET "
	return builder
}

func (builder *QueryBuilder) Set(hashMap *cypressutils.CypressHashMap) *QueryBuilder {
	err := validateUpdateSet(hashMap)
	if err != nil {
		return builder
	}

	builder.query += concatenateUpdateSet(hashMap)
	return builder
}

func (builder *QueryBuilder) SpecialSet(hashMap *cypressutils.CypressHashMap) *QueryBuilder {
	builder.query += concatenateUpdateSet(hashMap)
	return builder
}

func (builder *QueryBuilder) Returning(returningPhrase string) *QueryBuilder {
	if strings.TrimSpace(returningPhrase) == "" {
		err := cErrors.New("RETURNING STATEMENT: returning statement should not be empty")
		exceptions.ThrowException(err)
		builder.Err = err
		return builder
	}
	builder.query += " RETURNING " + returningPhrase
	return builder
}

/*public Query setTermination() throws QueryBuilderException {
      if (updatedValues[0].isEmpty()) throw new QueryBuilderException("UPDATE: Update set values missing in query: " + query);
      query += updatedValues[0];
      return this;
  }
*/
/*--------------------------------START OF DELETE QUERIES-------------------------------------*/

func (builder *QueryBuilder) DeleteFrom(tableName string) *QueryBuilder {
	err := validateTableName(tableName, "DELETE: Table name is empty")
	if err != nil {
		return builder
	}

	builder.tableName = tableName
	builder.query += "DELETE FROM " + tableName + " "
	return builder
}

/*--------------------------------START OF SELECT QUERIES-------------------------------------*/

func (builder *QueryBuilder) Select() *QueryBuilder {
	builder.query += "SELECT "
	return builder
}

func (builder *QueryBuilder) RawQuery(rawQuery string) *QueryBuilder {
	builder.query += rawQuery + " "
	return builder
}

func (builder *QueryBuilder) SelectColumn(column string) *QueryBuilder {
	builder.fetchColumnsList = append(builder.fetchColumnsList, &Column{ColumnName: column})
	builder.fetchedColumns[0] = concatenateColumnNamesForFetch(builder)
	builder.query += builder.fetchedColumns[0]
	return builder
}

func (builder *QueryBuilder) FromTable(tableName string) *QueryBuilder {
	err := validateTableName(tableName, "SELECT: Table name is empty")

	if err != nil {
		return builder
	}

	builder.tableName = tableName
	builder.query += " FROM " + tableName + " "
	return builder
}

func (builder *QueryBuilder) From() *QueryBuilder {
	builder.query += " FROM "
	return builder
}

func (builder *QueryBuilder) WherePred(filterPredicate *FilterPredicate) *QueryBuilder {
	return builder.WhereStr(filterPredicate.GetClause())
}

func (builder *QueryBuilder) WhereStr(whereClause string) *QueryBuilder {
	err := validateSelection(whereClause)
	if err != nil {
		return builder
	}

	builder.whereClause = whereClause

	builder.query += " WHERE " + whereClause + " "
	return builder
}

func (builder *QueryBuilder) GroupBy(columns string) *QueryBuilder {
	if columns == "" {
		err := cErrors.New("GROUP: group by columns must not be empty")
		exceptions.ThrowException(err)
		builder.Err = err
		return builder
	}
	builder.query += " GROUP BY " + columns + " "
	return builder
}

func (builder *QueryBuilder) HavingPred(filterPredicate *FilterPredicate) *QueryBuilder {
	return builder.HavingStr(filterPredicate.GetClause())
}

func (builder *QueryBuilder) HavingStr(havingClause string) *QueryBuilder {
	if havingClause == "" {
		err := cErrors.New("HAVING: Having clause is empty")
		exceptions.ThrowException(err)
		builder.Err = err
		return builder
	}
	builder.query += " HAVING " + havingClause + " "
	return builder
}

func (builder *QueryBuilder) OrderBy(orderBy string) *QueryBuilder {
	if orderBy == "" {
		err := cErrors.New("Order By clause cannot be empty")
		exceptions.ThrowException(err)
		builder.Err = err
		return builder
	}
	builder.query += " ORDER BY " + orderBy + " "
	return builder
}

/*********************************************************/

func (builder *QueryBuilder) Case() *QueryBuilder {
	builder.query += "(CASE "
	return builder
}

func (builder *QueryBuilder) End(succeedingComma ...bool) *QueryBuilder {
	builder.query += "END)"

	if succeedingComma != nil && succeedingComma[0] {
		builder.query += "), "
	} else {
		builder.query += ") "
	}
	return builder
}

func (builder *QueryBuilder) EndAs(alias string, succeedingComma ...bool) *QueryBuilder {
	builder.query += "END AS " + alias

	if succeedingComma != nil && succeedingComma[0] {
		builder.query += "), "
	} else {
		builder.query += ") "
	}

	return builder
}

func (builder *QueryBuilder) When(whenClause string) *QueryBuilder {
	builder.query += "WHEN " + whenClause + " "
	return builder
}

func (builder *QueryBuilder) Then(namedVariable string) *QueryBuilder {
	if !strings.HasPrefix(namedVariable, ":") {
		err := cErrors.New("THEN CLAUSE IN CASE: namedVariable must start with full colon[:]")
		exceptions.ThrowException(err)
		builder.Err = err
		return builder
	}
	if len(namedVariable) < 2 {
		err := cErrors.New("THEN CLAUSE IN CASE: namedVariable is empty")
		exceptions.ThrowException(err)
		builder.Err = err
		return builder
	}

	if strings.Count(namedVariable, ":") > 1 {
		err := cErrors.New("THEN CLAUSE IN CASE: namedVariable can only have one full colon[:]")
		exceptions.ThrowException(err)
		builder.Err = err
		return builder
	}

	builder.query += "THEN " + namedVariable + " "

	return builder
}

func (builder *QueryBuilder) Else(namedVariable ...string) *QueryBuilder {
	if namedVariable == nil {
		builder.query += " ELSE "
		return builder
	}

	if !strings.HasPrefix(namedVariable[0], ":") {
		err := cErrors.New("ELSE CLAUSE IN CASE: namedVariable must start with full colon[:]")
		exceptions.ThrowException(err)
		builder.Err = err
		return builder
	}

	if len(namedVariable[0]) < 2 {
		err := cErrors.New("ELSE CLAUSE IN CASE: namedVariable is empty")
		exceptions.ThrowException(err)
		builder.Err = err
		return builder
	}

	if strings.Count(namedVariable[0], ":") > 1 {
		err := cErrors.New("ELSE CLAUSE IN CASE: namedVariable can only have one full colon[:]")
		exceptions.ThrowException(err)
		builder.Err = err
		return builder
	}

	builder.query += "ELSE " + namedVariable[0] + " "

	return builder
}

func (builder *QueryBuilder) Limit() *QueryBuilder {
	builder.query += " LIMIT :num_of_records OFFSET :offset "
	return builder
}

/*********************************************************/

func (builder *QueryBuilder) Union(precedingQuery, followingQuery string) *QueryBuilder {
	builder.query += precedingQuery + " UNION " + followingQuery + " "
	return builder
}

func (builder *QueryBuilder) UnionAll(precedingQuery, followingQuery string) *QueryBuilder {
	builder.query += precedingQuery + " UNION ALL " + followingQuery + " "
	return builder
}

/******************************************************/

func (builder *QueryBuilder) JoinPhrase(joinPhrase string) *QueryBuilder {
	if strings.TrimSpace(joinPhrase) == "" {
		err := cErrors.New("JOIN STATEMENT: join statement should not be empty")
		exceptions.ThrowException(err)
		builder.Err = err
		return builder
	}
	builder.joinStatement += joinPhrase
	builder.query += joinPhrase
	return builder
}

func (builder *QueryBuilder) ToString() string {
	regexMatcher := regexp.MustCompile("\\s{2,}")

	result := strings.TrimSpace(builder.query)
	result = regexMatcher.ReplaceAllString(result, " ")

	return result
}

func (builder *QueryBuilder) DisplayQuery() {
	fmt.Println(FormatSQL(builder.query))
}

/*******************GETTERS AND SETTERS *************************/

func (builder *QueryBuilder) GetJoinStatement() string {
	return builder.joinStatement
}

func (builder *QueryBuilder) GetWhereClause() string {
	return builder.whereClause
}

func (builder *QueryBuilder) GetTableName() string {
	return builder.tableName
}

func (builder *QueryBuilder) GetPrimaryKeyColumns() []string {
	return builder.primaryKeyColumns
}

func (builder *QueryBuilder) GetPrimaryKeyColumn() string {
	return builder.primaryKeyColumn
}

func (builder *QueryBuilder) SetPrimaryKeyColumn(column string) *QueryBuilder {
	if strings.TrimSpace(column) == "" {
		err := cErrors.New("SetPrimaryKeyColumn: column cannot be empty")
		exceptions.ThrowException(err)
		builder.Err = err
		return builder
	}
	builder.primaryKeyColumn = column
	return builder
}

func (builder *QueryBuilder) AddPrimaryKeyColumns(column string) *QueryBuilder {
	if strings.TrimSpace(column) == "" {
		err := cErrors.New("AddPrimaryKeyColumns: column cannot be empty")
		exceptions.ThrowException(err)
		builder.Err = err
		return builder
	}
	builder.primaryKeyColumns = append(builder.primaryKeyColumns, column)
	return builder
}

/**************** UTILITY FUNCTIONS ****************/

func concatenateUpdateSet(hashmap *cypressutils.CypressHashMap) string {
	var buf bytes.Buffer
	shouldAddComma := false

	for pair := hashmap.GetData().Oldest(); pair != nil; pair = pair.Next() {
		if shouldAddComma {
			buf.WriteString(", ")
		} else {
			shouldAddComma = true
		}

		buf.WriteString(fmt.Sprintf("%v", pair.Key))
		buf.WriteString(" = ")
		buf.WriteString(fmt.Sprintf("%v", pair.Value))
	}
	return buf.String()
}

func concatenateColumnNamesForFetch(builder *QueryBuilder) string {
	var buf bytes.Buffer
	length := len(builder.fetchColumnsList)
	for index := 0; index < length; index++ {
		column := builder.fetchColumnsList[index]

		if column.Aggregate != NO_AGGREGATE {
			buf.WriteString(column.Aggregate.name())
			buf.WriteString("(")
			buf.WriteString(column.ColumnName)
			buf.WriteString(")")
		} else {
			buf.WriteString(column.ColumnName)
		}

		if column.ColumnAlias != "" {
			buf.WriteString(" AS ")
			buf.WriteString(column.ColumnAlias)
		}

		if index != length-1 {
			buf.WriteString(", ")
		}
	}
	return buf.String()
}

func concatenateColumnNames(columns []string) string {
	var buf bytes.Buffer

	shouldAddComma := false
	buf.WriteString("(")
	for index := range columns {
		if shouldAddComma {
			buf.WriteString(",")
		} else {
			shouldAddComma = true
		}
		buf.WriteString(columns[index])
	}
	buf.WriteString(")")
	return buf.String()
}

func concatenateOrderByColumnNames(columns []*ColumnOrderBy) string {
	var buf bytes.Buffer
	length := len(columns)
	for index := 0; index < length; index++ {
		columnOrderBy := columns[index]

		if columnOrderBy.Aggregate != NO_AGGREGATE {
			buf.WriteString(columnOrderBy.Aggregate.name())
			buf.WriteString("(")
			buf.WriteString(columnOrderBy.ColumnName)
			buf.WriteString(")")
		} else {
			buf.WriteString(columnOrderBy.ColumnName)
		}
		buf.WriteString(" ")
		buf.WriteString(columnOrderBy.OrderBy_.name())

		if index != length-1 {
			buf.WriteString(", ")
		}
	}
	return buf.String()
}

func validateOrderBy(columnOrderByList []*ColumnOrderBy) error {
	for _, orderBy := range columnOrderByList {
		if orderBy.ColumnName == "" {
			err := cErrors.New("ORDER BY: order by column cannot be empty")
			exceptions.ThrowException(err)
			return err
		}
	}
	return nil
}

func validateSelection(whereClause string) error {
	if whereClause == "" {
		err := cErrors.New("WHERE: where clause is empty")
		exceptions.ThrowException(err)
		return err
	}
	if strings.Count(whereClause, "(") != strings.Count(whereClause, ")") {
		err := cErrors.New("WHERE CLAUSE: Number of '(' do not match number of ')' in: " + whereClause)
		exceptions.ThrowException(err)
		return err
	}

	return nil
}

func validateTableName(tableName, errorMessage string) error {
	if tableName == "" {
		err := cErrors.New(errorMessage)
		exceptions.ThrowException(err)
		return err
	}
	return nil
}

func validateUpdateSet(hashmap *cypressutils.CypressHashMap) error {
	if hashmap == nil || hashmap.Size() == 0 {
		err := cErrors.New("UPDATE: Update set columns missing")
		exceptions.ThrowException(err)
		return err
	}

	for pair := hashmap.GetData().Oldest(); pair != nil; pair = pair.Next() {
		column := fmt.Sprintf("%v", pair.Key)
		namedVar := fmt.Sprintf("%v", pair.Value)

		if column == "" {
			err := cErrors.New("UPDATE: Update column is empty")
			exceptions.ThrowException(err)
			return err
		}

		if len(namedVar) < 2 {
			err := cErrors.New("UPDATE: named variable for update is empty")
			exceptions.ThrowException(err)
			return err
		}

		if !strings.HasPrefix(namedVar, ":") {
			err := cErrors.New("UPDATE: '" + namedVar + "' named variable must start with full colon[:] in SET " +
				"hashmap e.g. hashmap.put('name',':nameVar')")

			exceptions.ThrowException(err)
			return err
		}
	}
	return nil
}

func validateColumnArg(columnArg, message string) error {
	if !strings.HasPrefix(columnArg, ":") {
		err := cErrors.New("'" + columnArg + "' named variable must start with full colon[:]")
		exceptions.ThrowException(err)
		return err
	}

	if len(columnArg) < 2 {
		err := cErrors.New("ColumnArg is empty")
		exceptions.ThrowException(err)
		return err
	}

	return nil
}
