package cypressutils

//NOTE: THIS CLASS AUTO-COMMITS ALL TRANSACTIONS

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	cErrors "github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
	_ "reflect"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
)

func insert(organizationId string, queryBuilder *QueryBuilder, queryArguments *CypressHashMap) (twrapper *TransactionWrapper) {
	twrapper = NewTransactionWrapper()

	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	if err := queryBuilder.Err; err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		return twrapper
	}

	dbConn, err := GetConnection(organizationId)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	defer dbConn.Close()

	tempQuery := queryBuilder.ToString() + " RETURNING *"

	_, err = validateQueryArguments(tempQuery, queryArguments)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		return twrapper
	}

	namedParameter := NewNamedParameterQuery(tempQuery, queryArguments)

	twrapper.AddQueryExecuted(tempQuery)
	if showSql, _ := ConfShowSQL(); showSql {
		fmt.Println(FormatSQL(tempQuery))
	}

	parsedQuery := namedParameter.GetParsedQuery()
	parsedParameters := namedParameter.GetParsedParameters()

	resRows, err := dbConn.Query(parsedQuery, parsedParameters...)

	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	columns, err := resRows.Columns()
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	columnTypes, err := resRows.ColumnTypes()
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	numOfColumns := len(columns)

	values := make([]sql.RawBytes, numOfColumns)
	scanArgs := make([]interface{}, numOfColumns)

	//HERE IT STORES THE MEMORY ADDRESSES OF THE EXPECTED STORAGES OF THE RESULT VALUES.
	//CREATOR APPEARS TO BE A GENIUS
	for i := range values {
		scanArgs[i] = &values[i]
	}

	hashMap := NewMap()
	for resRows.Next() {
		err = resRows.Scan(scanArgs...)
		if err != nil {
			twrapper.SetHasErrors(true)
			twrapper.AddError(err.Error())
			ThrowException(cErrors.Cause(err))
			return twrapper
		}

		for i, value := range values {
			ParseSQLRawBytesToType(value, columns[i], columnTypes[i], hashMap)
		}
	}

	if err = resRows.Err(); err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	twrapper.SetData(hashMap)

	//Stats Give the Connection Pool Details. Lol. Found them by accident
	//dbConn.Stats().MaxLifetimeClosed
	return twrapper
}

func batchInsert(organizationId string, queryBuilder *QueryBuilder, queryArgsList *CypressArrayList) (twrapper *TransactionWrapper) {
	twrapper = NewTransactionWrapper(false)

	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	if err := queryBuilder.Err; err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		return twrapper
	}

	dbConn, err := GetConnection(organizationId)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	defer dbConn.Close()

	trx, err := dbConn.Begin()
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	parsedQueryParams := []interface{}{}
	shouldAddMAXComma := false

	var insertValuesQMark bytes.Buffer

	length := queryArgsList.Size()
	for i := 0; i < length; i++ {
		if shouldAddMAXComma {
			insertValuesQMark.WriteString(",")
		} else {
			shouldAddMAXComma = true
		}

		insertValuesQMark.WriteString("(")
		hashMap := queryArgsList.GetRecord(i)
		shouldAddMinComma := false
		for pair := hashMap.GetData().Oldest(); pair != nil; pair = pair.Next() {
			//column := fmt.Sprintf("%v", pair.Key)
			parsedQueryParams = append(parsedQueryParams, pair.Value)
			if shouldAddMinComma {
				insertValuesQMark.WriteString(",")
			} else {
				shouldAddMinComma = true
			}
			insertValuesQMark.WriteString("?")
		}

		insertValuesQMark.WriteString(")")
	}

	queryBuilder.ValuesConcatenated(insertValuesQMark.String())

	tempQuery := queryBuilder.ToString()

	twrapper.AddQueryExecuted(tempQuery)
	if showSql, _ := ConfShowSQL(); showSql {
		fmt.Println(FormatSQL(tempQuery))
	}

	_, err = trx.Exec(tempQuery, parsedQueryParams...)
	if err != nil {
		err2 := trx.Rollback()
		if err2 != nil {
			twrapper.SetHasErrors(true)
			twrapper.AddError(err.Error())
			logrus.Error(err2)
		}

		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	err = trx.Commit()
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	twrapper.SetData(true)
	//Stats Give the Connection Pool Details. Lol. Found them by accident

	//dbConn.Stats().MaxLifetimeClosed
	return twrapper
}

func getAutoIncrementPrimaryKey(organizationId string, dbConn *sql.DB, tableName string) (string, error) {
	cypressList, err := getPrimaryKeyColumns(organizationId, dbConn, tableName)
	if err != nil {
		return "", err
	}

	length := cypressList.Size()
	for index := 0; index < length; index++ {
		hashMap := cypressList.GetRecord(index)
		if strings.Contains(hashMap.GetStringValueOrIfNull("column_default", ""), "nextval") {
			return hashMap.GetStringValue("column_name"), nil
		}
	}

	return "", nil
}

func getPrimaryKeyColumns(organizationId string, dbConn *sql.DB, tableName string) (*CypressArrayList, error) {
	databaseName := GetConDSN(organizationId).GetDatabaseName()

	arr := strings.Split(tableName, ".")

	if len(arr) < 2 {
		return nil, errors.New("missing schema in table name")
	}

	strSQL := "SELECT kcu.column_name AS column_name, c.column_default\n" +
		"   FROM information_schema.table_constraints tco\n" +
		"         JOIN information_schema.key_column_usage kcu\n" +
		"              ON kcu.constraint_name = tco.constraint_name\n" +
		"                  AND kcu.constraint_schema = tco.constraint_schema\n" +
		"         JOIN information_schema.columns c\n" +
		"              ON kcu.column_name = c.column_name\n" +
		"                  AND kcu.table_schema = c.table_schema\n" +
		"                  AND kcu.table_name = c.table_name\n" +
		"\n" +
		"   WHERE tco.constraint_type = 'PRIMARY KEY'\n" +
		"       AND kcu.table_catalog = :database_name\n" +
		"       AND kcu.table_schema = :schema_name\n" +
		"       AND kcu.table_name = :table_name"

	queryArguments := NewMap()
	queryArguments.PutValue(":database_name", databaseName)
	queryArguments.PutValue(":schema_name", arr[0])
	queryArguments.PutValue(":table_name", arr[1])

	namedParameter := NewNamedParameterQuery(strSQL, queryArguments)

	parsedQuery := namedParameter.GetParsedQuery()
	parsedParameters := namedParameter.GetParsedParameters()

	resRows, err := dbConn.Query(parsedQuery, parsedParameters...)
	if err != nil {
		return nil, err
	}
	defer resRows.Close()

	columns, err := resRows.Columns()
	if err != nil {
		return nil, err
	}

	columnTypes, err := resRows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	numOfColumns := len(columns)

	values := make([]sql.RawBytes, numOfColumns)
	scanArgs := make([]interface{}, numOfColumns)

	//HERE IT STORES THE MEMORY ADDRESSES OF THE EXPECTED STORAGES OF THE RESULT VALUES.
	//CREATOR APPEARS TO BE A GENIUS
	for i := range values {
		scanArgs[i] = &values[i]
	}

	cypressList := NewList()

	for resRows.Next() {
		err = resRows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		hashMap := NewMap()

		for i, value := range values {
			/*if value == nil {
				hashMap.PutValue(columns[i], "")
			} else {
				hashMap.PutValue(columns[i], string(value))
			}*/

			ParseSQLRawBytesToType(value, columns[i], columnTypes[i], hashMap)
		}

		cypressList.AddNewRecord(hashMap)
	}

	if err = resRows.Err(); err != nil {
		return nil, err
	}
	return cypressList, nil
}

func RawQuery(organizationId string, query string, queryArguments *CypressHashMap) (twrapper *TransactionWrapper) {
	twrapper = NewTransactionWrapper(false)

	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	dbConn, err := GetConnection(organizationId)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	defer dbConn.Close()

	_, err = validateQueryArguments(query, queryArguments)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		return twrapper
	}

	twrapper.AddQueryExecuted(query)
	if showSql, _ := ConfShowSQL(); showSql {
		fmt.Println(FormatSQL(query))
	}

	namedParameter := NewNamedParameterQuery(query, queryArguments)
	parsedSelectQuery := namedParameter.GetParsedQuery()
	parsedSelectParameters := namedParameter.GetParsedParameters()

	_, err = dbConn.Exec(parsedSelectQuery, parsedSelectParameters...)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	twrapper.SetData(true)

	return twrapper
}

func selectData(organizationId string, query string, queryArguments *CypressHashMap) (twrapper *TransactionWrapper) {
	twrapper = NewTransactionWrapper()

	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	dbConn, err := GetConnection(organizationId)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	defer dbConn.Close()

	_, err = validateQueryArguments(query, queryArguments)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		return twrapper
	}

	twrapper.AddQueryExecuted(query)
	if showSql, _ := ConfShowSQL(); showSql {
		fmt.Println(FormatSQL(query))
	}

	namedParameter := NewNamedParameterQuery(query, queryArguments)
	parsedSelectQuery := namedParameter.GetParsedQuery()
	parsedSelectParameters := namedParameter.GetParsedParameters()

	resRows, err := dbConn.Query(parsedSelectQuery, parsedSelectParameters...)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	defer resRows.Close()

	columns, err := resRows.Columns()
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	columnTypes, err := resRows.ColumnTypes()
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	numOfColumns := len(columns)

	values := make([]sql.RawBytes, numOfColumns)
	scanArgs := make([]interface{}, numOfColumns)

	//HERE IT STORES THE MEMORY ADDRESSES OF THE EXPECTED STORAGES OF THE RESULT VALUES.
	//CREATOR APPEARS TO BE A GENIUS
	for i := range values {
		scanArgs[i] = &values[i]
	}

	cypressList := NewList()

	for resRows.Next() {
		err = resRows.Scan(scanArgs...)
		if err != nil {
			twrapper.SetHasErrors(true)
			twrapper.AddError(err.Error())
			ThrowException(cErrors.Cause(err))
			return twrapper
		}

		hashMap := NewMap()

		for i, value := range values {

			ParseSQLRawBytesToType(value, columns[i], columnTypes[i], hashMap)
		}

		cypressList.AddNewRecord(hashMap)
	}

	if err = resRows.Err(); err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	twrapper.SetData(cypressList)
	return twrapper
}

func update(organizationId string, queryBuilder *QueryBuilder, queryArguments *CypressHashMap) (twrapper *TransactionWrapper) {
	twrapper = NewTransactionWrapper()

	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	if err := queryBuilder.Err; err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		return twrapper
	}

	dbConn, err := GetConnection(organizationId)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	defer dbConn.Close()

	_, err = validateQueryArguments(queryBuilder.ToString(), queryArguments)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		return twrapper
	}

	if showSql, _ := ConfShowSQL(); showSql {
		fmt.Println(FormatSQL(queryBuilder.ToString()))
	}

	namedParameter := NewNamedParameterQuery(queryBuilder.ToString(), queryArguments)
	parsedSelectQuery := namedParameter.GetParsedQuery()
	parsedSelectParameters := namedParameter.GetParsedParameters()

	resRows, err := dbConn.Query(parsedSelectQuery, parsedSelectParameters...)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	defer resRows.Close()

	columns, err := resRows.Columns()
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	columnTypes, err := resRows.ColumnTypes()
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	numOfColumns := len(columns)

	values := make([]sql.RawBytes, numOfColumns)
	scanArgs := make([]interface{}, numOfColumns)

	//HERE IT STORES THE MEMORY ADDRESSES OF THE EXPECTED STORAGES OF THE RESULT VALUES.
	//CREATOR APPEARS TO BE A GENIUS
	for i := range values {
		scanArgs[i] = &values[i]
	}

	cypressList := NewList()

	for resRows.Next() {
		err = resRows.Scan(scanArgs...)
		if err != nil {
			twrapper.SetHasErrors(true)
			twrapper.AddError(err.Error())
			ThrowException(cErrors.Cause(err))
			return twrapper
		}

		hashMap := NewMap()

		for i, value := range values {
			ParseSQLRawBytesToType(value, columns[i], columnTypes[i], hashMap)
		}

		cypressList.AddNewRecord(hashMap)
	}

	if err = resRows.Err(); err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	twrapper.SetData(cypressList)
	return twrapper
}

func deleteData(organizationId string, queryBuilder *QueryBuilder, queryArguments *CypressHashMap) (twrapper *TransactionWrapper) {
	twrapper = NewTransactionWrapper()

	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	if err := queryBuilder.Err; err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		return twrapper
	}

	dbConn, err := GetConnection(organizationId)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	defer dbConn.Close()

	_, err = validateQueryArguments(queryBuilder.ToString(), queryArguments)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		return twrapper
	}

	if showSql, _ := ConfShowSQL(); showSql {
		fmt.Println(FormatSQL(queryBuilder.ToString()))
	}

	namedParameter := NewNamedParameterQuery(queryBuilder.ToString()+" RETURNING *", queryArguments)
	parsedSelectQuery := namedParameter.GetParsedQuery()
	parsedSelectParameters := namedParameter.GetParsedParameters()

	resRows, err := dbConn.Query(parsedSelectQuery, parsedSelectParameters...)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	defer resRows.Close()

	columns, err := resRows.Columns()
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	columnTypes, err := resRows.ColumnTypes()
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	numOfColumns := len(columns)

	values := make([]sql.RawBytes, numOfColumns)
	scanArgs := make([]interface{}, numOfColumns)

	//HERE IT STORES THE MEMORY ADDRESSES OF THE EXPECTED STORAGES OF THE RESULT VALUES.
	//CREATOR APPEARS TO BE A GENIUS
	for i := range values {
		scanArgs[i] = &values[i]
	}

	cypressList := NewList()

	for resRows.Next() {
		err = resRows.Scan(scanArgs...)
		if err != nil {
			twrapper.SetHasErrors(true)
			twrapper.AddError(err.Error())
			ThrowException(cErrors.Cause(err))
			return twrapper
		}

		hashMap := NewMap()

		for i, value := range values {
			ParseSQLRawBytesToType(value, columns[i], columnTypes[i], hashMap)
		}

		cypressList.AddNewRecord(hashMap)
	}

	if err = resRows.Err(); err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	twrapper.SetData(cypressList)
	return twrapper
}

func validateQueryArguments(query string, queryArguments *CypressHashMap) (*Set, error) {
	allQueryVariables := NewSet()

	/*
		Invalid (?<!abc)def is equivalent to valid (?:(^|[^a])(^|[^b])(^|[^c]))def
	*/

	regexMatcher := regexp.MustCompile("[^:]:[a-zA-Z\\d_.]+")

	matches := regexMatcher.FindAllString(query, -1)
	for _, queryVar := range matches {

		regexMatcher2 := regexp.MustCompile(":[a-zA-Z\\d_.]+")

		queryVar = regexMatcher2.FindString(queryVar)
		queryVar = strings.TrimSpace(queryVar)

		if !queryArguments.Contains(queryVar) {
			err := cErrors.New(queryVar + " value not provided in query arguments")
			ThrowException(err)
			return nil, err
		}
		allQueryVariables.Add(queryVar)
	}

	return allQueryVariables, nil
}

func validatePagePageSize(pagePageSize []int) error {
	if len(pagePageSize) != 2 {
		err := cErrors.New("LIMIT: Page and page size variables must be provided [page, page size]")
		ThrowException(err)
	}
	return nil
}

func ParseSQLRawBytesToType(value []byte, columnName string, columnType *sql.ColumnType, hashMap *CypressHashMap) {
	switch columnType.DatabaseTypeName() {
	/*case :
	{
		if value == nil {
			hashMap.PutValue(columnName, 0)
		} else {
			temp := new(big.Int)
			temp.SetBytes(value)
			hashMap.PutValue(columnName, temp)
		}
	}*/
	case "INT", "BIGINT", "INT4":
		{
			if value == nil {
				hashMap.PutValue(columnName, 0)
			} else {
				hashMap.PutValue(columnName, BytesToInt64(value))
			}
		}
	case "DOUBLE", "FLOAT", "DECIMAL", "FLOAT8":
		{
			if value == nil {
				hashMap.PutValue(columnName, 0)
			} else {
				temp, _ := decimal.NewFromString(string(value))
				temp2, _ := temp.Float64()
				hashMap.PutValue(columnName, temp2)
			}
		}
	case "BOOL":
		{
			if value == nil {
				hashMap.PutValue(columnName, false)
			} else {
				b1, _ := strconv.ParseBool(string(value))
				hashMap.PutValue(columnName, b1)
			}
		}
	default:
		{
			if value == nil {
				hashMap.PutValue(columnName, nil)
			} else {
				hashMap.PutValue(columnName, string(value))
			}
		}
	}

	/*.(value)

	fmt.Printf("Is zero %T", reflect.TypeOf(value).Kind())

	reflect.ValueOf(value)*/

	/*fmt.Printf("The Bytes %s", string(value))
	temp := reflect.Value{}

	if temp.IsZero() {

	}


	temp.SetBytes(value)
	temp = (temp).Convert(columnType.ScanType())
	fmt.Println("New Value", temp)*/
}
