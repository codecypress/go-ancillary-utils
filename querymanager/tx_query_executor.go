package querymanager

//THIS CLASS BEGINS AND ENDS TRANSACTIONS BUT LEAVES IT UPON THE USER TO DO THE COMMITS

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"github.com/codecypress/go-ancillary-utils/cypressutils"
	"github.com/codecypress/go-ancillary-utils/exceptions"
	"github.com/codecypress/go-ancillary-utils/xmlutils"
	cErrors "github.com/pkg/errors"
	"strings"
)

func txInsert(organizationId string, tx *sql.Tx, queryBuilder *QueryBuilder, queryArguments *cypressutils.CypressHashMap) (twrapper *cypressutils.TransactionWrapper, err error) {
	twrapper = cypressutils.NewTransactionWrapper()

	if err := queryBuilder.Err; err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		return twrapper, err
	}

	tempQuery := queryBuilder.ToString() + " RETURNING *"

	validateQueryArguments(tempQuery, queryArguments)

	namedParameter := NewNamedParameterQuery(tempQuery, queryArguments)

	twrapper.AddQueryExecuted(tempQuery)
	if showSql, _ := xmlutils.ConfShowSQL(); showSql {
		fmt.Println(FormatSQL(tempQuery))
	}

	parsedQuery := namedParameter.GetParsedQuery()
	parsedParameters := namedParameter.GetParsedParameters()

	resRows, err := tx.Query(parsedQuery, parsedParameters...)

	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	columns, err := resRows.Columns()
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	columnTypes, err := resRows.ColumnTypes()
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	numOfColumns := len(columns)

	values := make([]sql.RawBytes, numOfColumns)
	scanArgs := make([]interface{}, numOfColumns)

	//HERE IT STORES THE MEMORY ADDRESSES OF THE EXPECTED STORAGES OF THE RESULT VALUES.
	//CREATOR APPEARS TO BE A GENIUS
	for i := range values {
		scanArgs[i] = &values[i]
	}

	hashMap := cypressutils.NewMap()
	for resRows.Next() {
		err = resRows.Scan(scanArgs...)
		if err != nil {
			twrapper.SetHasErrors(true)
			twrapper.AddError(err.Error())
			exceptions.ThrowException(cErrors.Cause(err))
			return twrapper, err
		}

		for i, value := range values {
			ParseSQLRawBytesToType(value, columns[i], columnTypes[i], hashMap)
		}
	}

	if err = resRows.Err(); err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	twrapper.SetData(hashMap)

	//Stats Give the Connection Pool Details. Lol. Found them by accident
	//dbConn.Stats().MaxLifetimeClosed
	return twrapper, nil
}

func txBatchInsert(organizationId string, tx *sql.Tx, queryBuilder *QueryBuilder, queryArgsList *cypressutils.CypressArrayList) (twrapper *cypressutils.TransactionWrapper, err error) {
	twrapper = cypressutils.NewTransactionWrapper(false)

	if err := queryBuilder.Err; err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		return twrapper, err
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
	if showSql, _ := xmlutils.ConfShowSQL(); showSql {
		fmt.Println(FormatSQL(tempQuery))
	}

	_, err = tx.Exec(tempQuery, parsedQueryParams...)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	twrapper.SetData(true)
	//Stats Give the Connection Pool Details. Lol. Found them by accident
	//dbConn.Stats().MaxLifetimeClosed
	return twrapper, nil
}

func trxGetAutoIncrementPrimaryKey(organizationId string, tx *sql.Tx, tableName string) (string, error) {
	cypressList, err := _txGetPrimaryKeyColumns_(organizationId, tx, tableName)
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

func _txGetPrimaryKeyColumns_(organizationId string, tx *sql.Tx, tableName string) (*cypressutils.CypressArrayList, error) {
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

	queryArguments := cypressutils.NewMap()
	queryArguments.PutValue(":database_name", databaseName)
	queryArguments.PutValue(":schema_name", arr[0])
	queryArguments.PutValue(":table_name", arr[1])

	namedParameter := NewNamedParameterQuery(strSQL, queryArguments)

	parsedQuery := namedParameter.GetParsedQuery()
	parsedParameters := namedParameter.GetParsedParameters()

	resRows, err := tx.Query(parsedQuery, parsedParameters...)
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

	cypressList := cypressutils.NewList()

	for resRows.Next() {
		err = resRows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		hashMap := cypressutils.NewMap()

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

func txGetPrimaryKeyColumns(organizationId string, tx *sql.Tx, tableName string) (twrapper *cypressutils.TransactionWrapper, err error) {
	twrapper = cypressutils.NewTransactionWrapper()

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

	queryArguments := cypressutils.NewMap()
	queryArguments.PutValue(":database_name", databaseName)
	queryArguments.PutValue(":schema_name", arr[0])
	queryArguments.PutValue(":table_name", arr[1])

	namedParameter := NewNamedParameterQuery(strSQL, queryArguments)

	parsedQuery := namedParameter.GetParsedQuery()
	parsedParameters := namedParameter.GetParsedParameters()

	resRows, err := tx.Query(parsedQuery, parsedParameters...)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}
	defer resRows.Close()

	columns, err := resRows.Columns()
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	columnTypes, err := resRows.ColumnTypes()
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	numOfColumns := len(columns)

	values := make([]sql.RawBytes, numOfColumns)
	scanArgs := make([]interface{}, numOfColumns)

	//HERE IT STORES THE MEMORY ADDRESSES OF THE EXPECTED STORAGES OF THE RESULT VALUES.
	//CREATOR APPEARS TO BE A GENIUS
	for i := range values {
		scanArgs[i] = &values[i]
	}

	cypressList := cypressutils.NewList()

	for resRows.Next() {
		err = resRows.Scan(scanArgs...)
		if err != nil {
			twrapper.SetHasErrors(true)
			twrapper.AddError(err.Error())
			exceptions.ThrowException(cErrors.Cause(err))
			return twrapper, err
		}

		hashMap := cypressutils.NewMap()

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
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	twrapper.SetData(cypressList)

	return twrapper, nil
}

func txRawQuery(organizationId string, tx *sql.Tx, query string, queryArguments *cypressutils.CypressHashMap) (twrapper *cypressutils.TransactionWrapper, err error) {
	twrapper = cypressutils.NewTransactionWrapper(false)

	validateQueryArguments(query, queryArguments)

	twrapper.AddQueryExecuted(query)
	if showSql, _ := xmlutils.ConfShowSQL(); showSql {
		fmt.Println(FormatSQL(query))
	}

	namedParameter := NewNamedParameterQuery(query, queryArguments)
	parsedSelectQuery := namedParameter.GetParsedQuery()
	parsedSelectParameters := namedParameter.GetParsedParameters()

	_, err = tx.Exec(parsedSelectQuery, parsedSelectParameters...)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	twrapper.SetData(true)

	return twrapper, nil
}

func txSelectData(organizationId string, tx *sql.Tx, query string, queryArguments *cypressutils.CypressHashMap) (twrapper *cypressutils.TransactionWrapper, err error) {
	twrapper = cypressutils.NewTransactionWrapper()

	validateQueryArguments(query, queryArguments)

	twrapper.AddQueryExecuted(query)
	if showSql, _ := xmlutils.ConfShowSQL(); showSql {
		fmt.Println(FormatSQL(query))
	}

	namedParameter := NewNamedParameterQuery(query, queryArguments)
	parsedSelectQuery := namedParameter.GetParsedQuery()
	parsedSelectParameters := namedParameter.GetParsedParameters()

	resRows, err := tx.Query(parsedSelectQuery, parsedSelectParameters...)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	defer resRows.Close()

	columns, err := resRows.Columns()
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	columnTypes, err := resRows.ColumnTypes()
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	numOfColumns := len(columns)

	values := make([]sql.RawBytes, numOfColumns)
	scanArgs := make([]interface{}, numOfColumns)

	//HERE IT STORES THE MEMORY ADDRESSES OF THE EXPECTED STORAGES OF THE RESULT VALUES.
	//CREATOR APPEARS TO BE A GENIUS
	for i := range values {
		scanArgs[i] = &values[i]
	}

	cypressList := cypressutils.NewList()

	for resRows.Next() {
		err = resRows.Scan(scanArgs...)
		if err != nil {
			twrapper.SetHasErrors(true)
			twrapper.AddError(err.Error())
			exceptions.ThrowException(cErrors.Cause(err))
			return twrapper, err
		}

		hashMap := cypressutils.NewMap()

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
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	twrapper.SetData(cypressList)
	return twrapper, nil
}

func txUpdate(organizationId string, tx *sql.Tx, queryBuilder *QueryBuilder, queryArguments *cypressutils.CypressHashMap) (twrapper *cypressutils.TransactionWrapper, err error) {
	twrapper = cypressutils.NewTransactionWrapper()

	if err := queryBuilder.Err; err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		return twrapper, err
	}

	_, err = validateQueryArguments(queryBuilder.ToString(), queryArguments)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		return twrapper, err
	}

	if showSql, _ := xmlutils.ConfShowSQL(); showSql {
		fmt.Println(FormatSQL(queryBuilder.ToString()))
	}

	namedParameter := NewNamedParameterQuery(queryBuilder.ToString(), queryArguments)
	parsedSelectQuery := namedParameter.GetParsedQuery()
	parsedSelectParameters := namedParameter.GetParsedParameters()

	resRows, err := tx.Query(parsedSelectQuery, parsedSelectParameters...)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	defer resRows.Close()

	columns, err := resRows.Columns()
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	columnTypes, err := resRows.ColumnTypes()
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	numOfColumns := len(columns)

	values := make([]sql.RawBytes, numOfColumns)
	scanArgs := make([]interface{}, numOfColumns)

	//HERE IT STORES THE MEMORY ADDRESSES OF THE EXPECTED STORAGES OF THE RESULT VALUES.
	//CREATOR APPEARS TO BE A GENIUS
	for i := range values {
		scanArgs[i] = &values[i]
	}

	cypressList := cypressutils.NewList()

	for resRows.Next() {
		err = resRows.Scan(scanArgs...)
		if err != nil {
			twrapper.SetHasErrors(true)
			twrapper.AddError(err.Error())
			exceptions.ThrowException(cErrors.Cause(err))
			return twrapper, err
		}

		hashMap := cypressutils.NewMap()

		for i, value := range values {
			ParseSQLRawBytesToType(value, columns[i], columnTypes[i], hashMap)
		}

		cypressList.AddNewRecord(hashMap)
	}

	if err = resRows.Err(); err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	twrapper.SetData(cypressList)
	return twrapper, nil
}

func txDelete(organizationId string, tx *sql.Tx, queryBuilder *QueryBuilder, queryArguments *cypressutils.CypressHashMap) (twrapper *cypressutils.TransactionWrapper, err error) {
	twrapper = cypressutils.NewTransactionWrapper()

	if err := queryBuilder.Err; err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		return twrapper, err
	}

	_, err = validateQueryArguments(queryBuilder.ToString(), queryArguments)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		return twrapper, err
	}

	if showSql, _ := xmlutils.ConfShowSQL(); showSql {
		fmt.Println(FormatSQL(queryBuilder.ToString()))
	}

	namedParameter := NewNamedParameterQuery(queryBuilder.ToString()+" RETURNING *", queryArguments)
	parsedSelectQuery := namedParameter.GetParsedQuery()
	parsedSelectParameters := namedParameter.GetParsedParameters()

	resRows, err := tx.Query(parsedSelectQuery, parsedSelectParameters...)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	defer resRows.Close()

	columns, err := resRows.Columns()
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	columnTypes, err := resRows.ColumnTypes()
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	numOfColumns := len(columns)

	values := make([]sql.RawBytes, numOfColumns)
	scanArgs := make([]interface{}, numOfColumns)

	//HERE IT STORES THE MEMORY ADDRESSES OF THE EXPECTED STORAGES OF THE RESULT VALUES.
	//CREATOR APPEARS TO BE A GENIUS
	for i := range values {
		scanArgs[i] = &values[i]
	}

	cypressList := cypressutils.NewList()

	for resRows.Next() {
		err = resRows.Scan(scanArgs...)
		if err != nil {
			twrapper.SetHasErrors(true)
			twrapper.AddError(err.Error())
			exceptions.ThrowException(cErrors.Cause(err))
			return twrapper, err
		}

		hashMap := cypressutils.NewMap()

		for i, value := range values {
			ParseSQLRawBytesToType(value, columns[i], columnTypes[i], hashMap)
		}

		cypressList.AddNewRecord(hashMap)
	}

	if err = resRows.Err(); err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	twrapper.SetData(cypressList)
	return twrapper, nil
}
