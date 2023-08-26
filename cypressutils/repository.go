package cypressutils

import (
	"fmt"
	cErrors "github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"math"
	"runtime/debug"
	"strconv"
)

func Insert(organizationId, tableName string, recordHashMap *CypressHashMap) (twrapper *TransactionWrapper) {
	queryArguments := NewMap()

	for pair := recordHashMap.GetData().Oldest(); pair != nil; pair = pair.Next() {
		field := fmt.Sprintf("%v", pair.Key)
		queryArguments.PutValue(":"+field, pair.Value)
	}

	queryBuilder := NewQueryBuilder()
	queryBuilder.Insert().Into(tableName).Columns(queryArguments.GetKeysNoStartColon()).Values(queryArguments.GetKeysWithStartColon())

	return insert(organizationId, queryBuilder, queryArguments)
}

func InsertFromMap(organizationId, tableName string, recordHashMap map[string]interface{}) (twrapper *TransactionWrapper) {
	queryArguments := NewMap()

	for key, _value := range recordHashMap {
		field := fmt.Sprintf("%v", key)
		queryArguments.PutValue(":"+field, _value)
	}

	queryBuilder := NewQueryBuilder()
	queryBuilder.Insert().Into(tableName).Columns(queryArguments.GetKeysNoStartColon()).Values(queryArguments.GetKeysWithStartColon())

	return insert(organizationId, queryBuilder, queryArguments)
}

func InsertOnDuplicate(organizationId, tableName string, recordHashMap *CypressHashMap, onDuplicateColumns []string) (twrapper *TransactionWrapper) {
	queryArguments := NewMap()

	for pair := recordHashMap.GetData().Oldest(); pair != nil; pair = pair.Next() {
		field := fmt.Sprintf("%v", pair.Key)
		queryArguments.PutValue(":"+field, pair.Value)
	}

	queryBuilder := NewQueryBuilder()
	queryBuilder.Insert().Into(tableName).Columns(queryArguments.GetKeysNoStartColon()).Values(queryArguments.GetKeysWithStartColon())
	if onDuplicateColumns != nil {
		queryBuilder.OnDuplicateKey(onDuplicateColumns)
	}

	return insert(organizationId, queryBuilder, queryArguments)
}

func InsertFromMapOnDuplicate(organizationId, tableName string, onDuplicateColumns []string, recordHashMap map[string]interface{}) (twrapper *TransactionWrapper) {
	queryArguments := NewMap()

	for key, _value := range recordHashMap {
		field := fmt.Sprintf("%v", key)
		queryArguments.PutValue(":"+field, _value)
	}

	queryBuilder := NewQueryBuilder()
	queryBuilder.Insert().Into(tableName).Columns(queryArguments.GetKeysNoStartColon()).Values(queryArguments.GetKeysWithStartColon())
	if onDuplicateColumns != nil {
		queryBuilder.OnDuplicateKey(onDuplicateColumns)
	}

	return insert(organizationId, queryBuilder, queryArguments)
}

func BatchInsert(organizationId, tableName string, queryArgsList *CypressArrayList) (twrapper *TransactionWrapper) {
	queryBuilder := NewQueryBuilder()
	queryBuilder.Insert().Into(tableName).Columns(queryArgsList.GetRecord(0).GetKeysNoStartColon())
	return batchInsert(organizationId, queryBuilder, queryArgsList)
}

func GetPrimaryKeyColumns(organizationId string, tableName string) (twrapper *TransactionWrapper) {
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

	list, err := getPrimaryKeyColumns(organizationId, dbConn, tableName)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		ThrowException(cErrors.Cause(err))
		return twrapper
	}

	twrapper.SetData(list)

	return twrapper
}

func Update(organizationId, tableName string, updateSet *CypressHashMap, filterPredicate *FilterPredicate,
	queryArguments *CypressHashMap, selectPreUpdate bool, pagePageSize []int) (twrapper *TransactionWrapper) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	twrapper = NewTransactionWrapper()
	if queryArguments == nil {
		queryArguments = NewMap()
	}
	queryArguments.SetTableName(tableName)

	if updateSet == nil || updateSet.IsEmpty() {
		twrapper.SetHasErrors(true)
		twrapper.AddError("UPDATE: Update set should not be empty")
		logrus.Error("UPDATE: Update set should not be empty")
		return twrapper
	}

	updateSetVariables := NewMap()
	for pair := updateSet.GetData().Oldest(); pair != nil; pair = pair.Next() {
		field := fmt.Sprintf("%v", pair.Key)
		updateSetVariables.PutValue(field, ":"+field)
		queryArguments.PutValue(":"+field, pair.Value)
	}

	if selectPreUpdate {

		primKeysWrapper := GetPrimaryKeyColumns(organizationId, tableName)
		if primKeysWrapper.HasErrors {
			twrapper.CopyFrom(primKeysWrapper)
			return twrapper
		}

		primaryKeyColsList := primKeysWrapper.Data.(*CypressArrayList)

		primListLength := primaryKeyColsList.Size()

		shouldAddAnd := false
		theONString := ""

		for index := 0; index < primListLength; index++ {
			hashMap := primaryKeyColsList.GetRecord(index)

			if shouldAddAnd {
				theONString += " AND "
			} else {
				shouldAddAnd = true
			}

			theONString += "nvls." + hashMap.GetStringValue("column_name") + " = ovls." + hashMap.GetStringValue("column_name") + " "
		}

		queryBuilder := NewQueryBuilder()

		if pagePageSize != nil {
			queryBuilder.Prepend("WITH the_updates AS (")
		}

		queryBuilder.Update(tableName + " nvls").
			Set(updateSetVariables).
			FromTable(tableName + " ovls")

		if filterPredicate != nil && filterPredicate.GetClause() != "" {
			queryBuilder.WhereStr(theONString + " AND " + filterPredicate.GetClause())
		} else {
			queryBuilder.WhereStr(theONString)
		}

		updateColumns := updateSetVariables.GetKeysNoStartColon()

		strOldValsCols := ""
		strNewValsCols := ""
		shouldAddComma := false

		for _, column := range updateColumns {
			if shouldAddComma {
				strOldValsCols += ", "
				strNewValsCols += ", "
			} else {
				shouldAddComma = true
			}
			strOldValsCols += "ovls." + column + " AS the_old_col_" + column
			strNewValsCols += "nvls." + column
		}

		queryBuilder.Returning(strNewValsCols + ", " + strOldValsCols)

		if pagePageSize != nil {
			queryBuilder.Append(")")
			queryBuilder.Select().SelectColumn("*").FromTable("the_updates")

			err := validatePagePageSize(pagePageSize)
			if err != nil {
				twrapper.SetHasErrors(true)
				twrapper.AddError(err.Error())
				ThrowException(cErrors.Cause(err))
				return twrapper
			}

			if pagePageSize[0] > 0 {
				queryArguments.AddQueryArgument(":num_of_records", pagePageSize[1])
				queryArguments.AddQueryArgument(":offset", (pagePageSize[0]-1)*pagePageSize[1])
				queryBuilder.Limit()
			}
		}

		return update(organizationId, queryBuilder, queryArguments)
	}

	queryBuilder := NewQueryBuilder()
	queryBuilder.Update(tableName).Set(updateSetVariables)

	if filterPredicate != nil && filterPredicate.GetClause() != "" {
		queryBuilder.WherePred(filterPredicate)
	}

	return update(organizationId, queryBuilder, queryArguments)
}

func Delete(organizationId, tableName string, filterPredicate *FilterPredicate, queryArguments *CypressHashMap) (twrapper *TransactionWrapper) {
	twrapper = NewTransactionWrapper()
	if queryArguments == nil {
		queryArguments = NewMap()
	}
	queryArguments.SetTableName(tableName)

	queryBuilder := NewQueryBuilder()
	queryBuilder.DeleteFrom(tableName)

	if filterPredicate != nil && filterPredicate.GetClause() != "" {
		queryBuilder.WherePred(filterPredicate)
	}

	return deleteData(organizationId, queryBuilder, queryArguments)
}

func JoinSelectQuery(organizationId string, queryBuilder *QueryBuilder, queryArguments *CypressHashMap, pagePageSize []int) (twrapper *TransactionWrapper) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	twrapper = NewTransactionWrapper()
	tableName := queryBuilder.GetTableName()
	if queryArguments == nil {
		queryArguments = NewMap()
	}
	queryArguments.SetTableName(tableName)

	qrQueryBuilder := NewQueryBuilder().RawQuery(queryBuilder.ToString())
	if pagePageSize != nil {
		err := validatePagePageSize(pagePageSize)
		if err != nil {
			twrapper.SetHasErrors(true)
			twrapper.AddError(err.Error())
			ThrowException(cErrors.Cause(err))
			return twrapper
		}

		if pagePageSize[0] > 0 {
			queryArguments.AddQueryArgument(":num_of_records", pagePageSize[1])
			queryArguments.AddQueryArgument(":offset", (pagePageSize[0]-1)*pagePageSize[1])
			qrQueryBuilder.Limit()
		}
	}

	twrapper = selectData(organizationId, qrQueryBuilder.ToString(), queryArguments)

	if pagePageSize != nil {
		if pagePageSize[0] > 0 {
			twrapper.SetData(paginate(tableName, twrapper.GetData(), JoinCountQuery(organizationId, queryBuilder, queryArguments),
				pagePageSize[0],
				pagePageSize[1],
			))
		}
	}

	return twrapper

}

func JoinCountQuery(organizationId string, queryBuilder *QueryBuilder, queryArguments *CypressHashMap) int {
	countQueryBuilder := NewQueryBuilder().Select().SelectColumn("COUNT(*) AS count")
	countQueryBuilder.From()
	countQueryBuilder.JoinPhrase(queryBuilder.GetJoinStatement())

	if queryBuilder.GetWhereClause() != "" {
		countQueryBuilder.WhereStr(queryBuilder.GetWhereClause())
	}

	twrapper := selectData(organizationId, countQueryBuilder.ToString(), queryArguments)
	cypressList := twrapper.GetData().(*CypressArrayList)
	hashMap := cypressList.GetRecord(0)
	count, _ := strconv.Atoi(hashMap.GetStringValue("count"))
	return count
}

func JoinExistsQuery(organizationId string, queryBuilder *QueryBuilder, queryArguments *CypressHashMap) bool {
	countQueryBuilder := NewQueryBuilder().Select().SelectColumn("1")
	countQueryBuilder.From()
	countQueryBuilder.JoinPhrase(queryBuilder.GetJoinStatement())
	countQueryBuilder.WhereStr(queryBuilder.GetWhereClause())

	twrapper := selectData(organizationId, countQueryBuilder.ToString(), queryArguments)
	cypressList := twrapper.GetData().(*CypressArrayList)
	return !(cypressList == nil || cypressList.Size() < 1)
}

func SelectWithQueryBuilder(organizationId string, queryBuilder *QueryBuilder, queryArguments *CypressHashMap, pagePageSize []int) (twrapper *TransactionWrapper) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	twrapper = NewTransactionWrapper()
	tableName := queryBuilder.GetTableName()
	if queryArguments == nil {
		queryArguments = NewMap()
	}
	queryArguments.SetTableName(tableName)

	qrQueryBuilder := NewQueryBuilder().RawQuery(queryBuilder.ToString())

	if pagePageSize != nil {
		err := validatePagePageSize(pagePageSize)
		if err != nil {
			twrapper.SetHasErrors(true)
			twrapper.AddError(err.Error())
			ThrowException(cErrors.Cause(err))
			return twrapper
		}

		if pagePageSize[0] > 0 {
			queryArguments.AddQueryArgument(":num_of_records", pagePageSize[1])
			queryArguments.AddQueryArgument(":offset", (pagePageSize[0]-1)*pagePageSize[1])
			qrQueryBuilder.Limit()
		}
	}

	twrapper = selectData(organizationId, qrQueryBuilder.ToString(), queryArguments)

	if pagePageSize != nil {
		if pagePageSize[0] > 0 {
			twrapper.SetData(paginate(tableName, twrapper.GetData(), Count(organizationId, queryBuilder, queryArguments),
				pagePageSize[0],
				pagePageSize[1],
			))
		}
	}

	return twrapper
}

func Count(organizationId string, queryBuilder *QueryBuilder, queryArguments *CypressHashMap) int {
	countQueryBuilder := NewQueryBuilder().Select().SelectColumn("COUNT(*) AS count")
	countQueryBuilder.FromTable(queryBuilder.GetTableName())
	if queryBuilder.GetWhereClause() != "" {
		countQueryBuilder.WhereStr(queryBuilder.GetWhereClause())
	}

	twrapper := selectData(organizationId, countQueryBuilder.ToString(), queryArguments)
	cypressList := twrapper.GetData().(*CypressArrayList)
	hashMap := cypressList.GetRecord(0)

	count, _ := strconv.Atoi(hashMap.GetStringValue("count"))
	return count
}

func Exists(organizationId string, queryBuilder *QueryBuilder, queryArguments *CypressHashMap) bool {
	countQueryBuilder := NewQueryBuilder().Select().SelectColumn("1")
	countQueryBuilder.FromTable(queryBuilder.GetTableName())
	countQueryBuilder.WhereStr(queryBuilder.GetWhereClause())

	twrapper := selectData(organizationId, countQueryBuilder.ToString(), queryArguments)
	cypressList := twrapper.GetData().(*CypressArrayList)
	return !(cypressList == nil || cypressList.Size() < 1)
}

func Select(organizationId, tableName, columns string, pagePageSize []int) (twrapper *TransactionWrapper) {
	defer func() {
		if r := recover(); r != nil {
			//exceptions.ThrowException(r)
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	twrapper = NewTransactionWrapper()
	queryArguments := NewMap()

	queryBuilder := NewQueryBuilder()
	queryBuilder.Select()
	if columns != "" {
		queryBuilder.SelectColumn(columns)
	}

	queryBuilder.FromTable(tableName)
	queryArguments.SetTableName(tableName)

	if pagePageSize != nil {
		err := validatePagePageSize(pagePageSize)
		if err != nil {
			twrapper.SetHasErrors(true)
			twrapper.AddError(err.Error())
			ThrowException(cErrors.Cause(err))
			return twrapper
		}

		if pagePageSize[0] > 0 {
			queryArguments.AddQueryArgument(":num_of_records", pagePageSize[1])
			queryArguments.AddQueryArgument(":offset", (pagePageSize[0]-1)*pagePageSize[1])
			queryBuilder.Limit()
		}
	}

	twrapper = selectData(organizationId, queryBuilder.ToString(), queryArguments)

	if pagePageSize != nil {
		if pagePageSize[0] > 0 {
			twrapper.SetData(paginate(tableName, twrapper.GetData(), Count(organizationId, queryBuilder, queryArguments),
				pagePageSize[0],
				pagePageSize[1],
			))
		}
	}
	return twrapper
}

func SelectOrderBy(organizationId, tableName, columns, columnOrderBy string, pagePageSize []int) (twrapper *TransactionWrapper) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	twrapper = NewTransactionWrapper()
	queryArguments := NewMap()

	queryBuilder := NewQueryBuilder()
	queryBuilder.Select()
	if columns != "" {
		queryBuilder.SelectColumn(columns)
	}

	queryBuilder.FromTable(tableName)
	queryArguments.SetTableName(tableName)

	if columnOrderBy != "" {
		queryBuilder.OrderBy(columnOrderBy)
	}

	if pagePageSize != nil {
		err := validatePagePageSize(pagePageSize)
		if err != nil {
			twrapper.SetHasErrors(true)
			twrapper.AddError(err.Error())
			ThrowException(cErrors.Cause(err))
			return twrapper
		}

		if pagePageSize[0] > 0 {
			queryArguments.AddQueryArgument(":num_of_records", pagePageSize[1])
			queryArguments.AddQueryArgument(":offset", (pagePageSize[0]-1)*pagePageSize[1])
			queryBuilder.Limit()
		}
	}

	twrapper = selectData(organizationId, queryBuilder.ToString(), queryArguments)

	if pagePageSize != nil {
		if pagePageSize[0] > 0 {
			twrapper.SetData(paginate(tableName, twrapper.GetData(), Count(organizationId, queryBuilder, queryArguments),
				pagePageSize[0],
				pagePageSize[1],
			))
		}
	}
	return twrapper
}

func SelectGroupBy(organizationId, tableName, columns, groupByColumns string, havingPredicate *FilterPredicate, queryArguments *CypressHashMap, pagePageSize []int) (twrapper *TransactionWrapper) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	twrapper = NewTransactionWrapper()

	if queryArguments == nil {
		queryArguments = NewMap()
	}
	queryArguments.SetTableName(tableName)

	queryBuilder := NewQueryBuilder()
	queryBuilder.Select()
	if columns != "" {
		queryBuilder.SelectColumn(columns)
	}

	queryBuilder.FromTable(tableName)

	if groupByColumns != "" {
		queryBuilder.GroupBy(groupByColumns)
	}
	if havingPredicate != nil {
		queryBuilder.HavingPred(havingPredicate)
	}

	if pagePageSize != nil {
		err := validatePagePageSize(pagePageSize)
		if err != nil {
			twrapper.SetHasErrors(true)
			twrapper.AddError(err.Error())
			ThrowException(cErrors.Cause(err))
			return twrapper
		}

		if pagePageSize[0] > 0 {
			queryArguments.AddQueryArgument(":num_of_records", pagePageSize[1])
			queryArguments.AddQueryArgument(":offset", (pagePageSize[0]-1)*pagePageSize[1])
			queryBuilder.Limit()
		}
	}

	twrapper = selectData(organizationId, queryBuilder.ToString(), queryArguments)

	if pagePageSize != nil {
		if pagePageSize[0] > 0 {
			twrapper.SetData(paginate(tableName, twrapper.GetData(),
				CountGroupBy(organizationId, tableName, groupByColumns, havingPredicate, queryArguments),
				pagePageSize[0],
				pagePageSize[1],
			))
		}
	}
	return twrapper
}

func SelectGroupByOrderBy(organizationId, tableName, columns, groupByColumns string, havingPredicate *FilterPredicate, columnOrderBy string, queryArguments *CypressHashMap, pagePageSize []int) (twrapper *TransactionWrapper) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	twrapper = NewTransactionWrapper()
	if queryArguments == nil {
		queryArguments = NewMap()
	}
	queryArguments.SetTableName(tableName)

	queryBuilder := NewQueryBuilder()
	queryBuilder.Select()
	if columns != "" {
		queryBuilder.SelectColumn(columns)
	}

	queryBuilder.FromTable(tableName)

	if groupByColumns != "" {
		queryBuilder.GroupBy(groupByColumns)
	}
	if havingPredicate != nil {
		queryBuilder.HavingPred(havingPredicate)
	}
	if columnOrderBy != "" {
		queryBuilder.OrderBy(columnOrderBy)
	}

	if pagePageSize != nil {
		err := validatePagePageSize(pagePageSize)
		if err != nil {
			twrapper.SetHasErrors(true)
			twrapper.AddError(err.Error())
			ThrowException(cErrors.Cause(err))
			return twrapper
		}

		if pagePageSize[0] > 0 {
			queryArguments.AddQueryArgument(":num_of_records", pagePageSize[1])
			queryArguments.AddQueryArgument(":offset", (pagePageSize[0]-1)*pagePageSize[1])
			queryBuilder.Limit()
		}
	}

	twrapper = selectData(organizationId, queryBuilder.ToString(), queryArguments)

	if pagePageSize != nil {
		if pagePageSize[0] > 0 {
			twrapper.SetData(paginate(tableName, twrapper.GetData(),
				CountGroupBy(organizationId, tableName, groupByColumns, havingPredicate, queryArguments),
				pagePageSize[0],
				pagePageSize[1],
			))
		}
	}
	return twrapper
}

func CountGroupBy(organizationId, tableName, groupByColumns string, havingPredicate *FilterPredicate, queryArguments *CypressHashMap) int {
	if queryArguments == nil {
		queryArguments = NewMap()
	}
	queryArguments.SetTableName(tableName)

	countQueryBuilder := NewQueryBuilder().Select().SelectColumn("COUNT(*) AS count")
	countQueryBuilder.FromTable(tableName)

	if groupByColumns != "" {
		countQueryBuilder.GroupBy(groupByColumns)
	}
	if havingPredicate != nil {
		countQueryBuilder.HavingPred(havingPredicate)
	}

	twrapper := selectData(organizationId, countQueryBuilder.ToString(), queryArguments)
	cypressList := twrapper.GetData().(*CypressArrayList)
	hashMap := cypressList.GetRecord(0)
	count, _ := strconv.Atoi(hashMap.GetStringValue("count"))
	return count
}

func SelectWhere(organizationId, tableName, columns string, filterPredicate *FilterPredicate, queryArguments *CypressHashMap, pagePageSize []int) (twrapper *TransactionWrapper) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	twrapper = NewTransactionWrapper()
	if queryArguments == nil {
		queryArguments = NewMap()
	}
	queryArguments.SetTableName(tableName)

	queryBuilder := NewQueryBuilder()
	queryBuilder.Select()
	if columns != "" {
		queryBuilder.SelectColumn(columns)
	}

	queryBuilder.FromTable(tableName)
	if filterPredicate != nil && filterPredicate.GetClause() != "" {
		queryBuilder.WherePred(filterPredicate)
	}

	if pagePageSize != nil {
		err := validatePagePageSize(pagePageSize)
		if err != nil {
			twrapper.SetHasErrors(true)
			twrapper.AddError(err.Error())
			ThrowException(cErrors.Cause(err))
			return twrapper
		}

		if pagePageSize[0] > 0 {
			queryArguments.AddQueryArgument(":num_of_records", pagePageSize[1])
			queryArguments.AddQueryArgument(":offset", (pagePageSize[0]-1)*pagePageSize[1])
			queryBuilder.Limit()
		}
	}

	twrapper = selectData(organizationId, queryBuilder.ToString(), queryArguments)

	if pagePageSize != nil {
		if pagePageSize[0] > 0 {
			twrapper.SetData(paginate(tableName, twrapper.GetData(), CountWhere(organizationId, tableName, filterPredicate, queryArguments),
				pagePageSize[0],
				pagePageSize[1],
			))
		}
	}
	return twrapper
}

func CountWhere(organizationId, tableName string, filterPredicate *FilterPredicate, queryArguments *CypressHashMap) int {
	if queryArguments == nil {
		queryArguments = NewMap()
	}
	queryArguments.SetTableName(tableName)

	countQueryBuilder := NewQueryBuilder().Select().SelectColumn("COUNT(*) AS count")
	countQueryBuilder.FromTable(tableName)

	if filterPredicate != nil && filterPredicate.GetClause() != "" {
		countQueryBuilder.WherePred(filterPredicate)
	}

	twrapper := selectData(organizationId, countQueryBuilder.ToString(), queryArguments)
	cypressList := twrapper.GetData().(*CypressArrayList)
	hashMap := cypressList.GetRecord(0)
	count, _ := strconv.Atoi(hashMap.GetStringValue("count"))
	return count
}

func SelectWhereOrderBy(organizationId, tableName, columns string,
	filterPredicate *FilterPredicate,
	columnOrderBy string,
	queryArguments *CypressHashMap,
	pagePageSize []int) (twrapper *TransactionWrapper) {

	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	twrapper = NewTransactionWrapper()
	if queryArguments == nil {
		queryArguments = NewMap()
	}
	queryArguments.SetTableName(tableName)

	queryBuilder := NewQueryBuilder()
	queryBuilder.Select()
	if columns != "" {
		queryBuilder.SelectColumn(columns)
	}

	queryBuilder.FromTable(tableName)
	if filterPredicate != nil && filterPredicate.GetClause() != "" {
		queryBuilder.WherePred(filterPredicate)
	}

	if columnOrderBy != "" {
		queryBuilder.OrderBy(columnOrderBy)
	}

	if pagePageSize != nil {
		err := validatePagePageSize(pagePageSize)
		if err != nil {
			twrapper.SetHasErrors(true)
			twrapper.AddError(err.Error())
			ThrowException(cErrors.Cause(err))
			return twrapper
		}

		if pagePageSize[0] > 0 {
			queryArguments.AddQueryArgument(":num_of_records", pagePageSize[1])
			queryArguments.AddQueryArgument(":offset", (pagePageSize[0]-1)*pagePageSize[1])
			queryBuilder.Limit()
		}
	}

	twrapper = selectData(organizationId, queryBuilder.ToString(), queryArguments)

	if pagePageSize != nil {
		if pagePageSize[0] > 0 {
			twrapper.SetData(paginate(tableName, twrapper.GetData(), CountWhere(organizationId, tableName, filterPredicate, queryArguments),
				pagePageSize[0],
				pagePageSize[1],
			))
		}
	}
	return twrapper
}

func SelectWhereGroupBy(organizationId, tableName, columns string,
	wherePredicate *FilterPredicate,
	groupByColumns string, havingPredicate *FilterPredicate,
	queryArguments *CypressHashMap,
	pagePageSize []int) (twrapper *TransactionWrapper) {

	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	twrapper = NewTransactionWrapper()
	if queryArguments == nil {
		queryArguments = NewMap()
	}
	queryArguments.SetTableName(tableName)

	queryBuilder := NewQueryBuilder()
	queryBuilder.Select()
	if columns != "" {
		queryBuilder.SelectColumn(columns)
	}

	queryBuilder.FromTable(tableName)

	if wherePredicate != nil && wherePredicate.GetClause() != "" {
		queryBuilder.WherePred(wherePredicate)
	}

	if groupByColumns != "" {
		queryBuilder.GroupBy(groupByColumns)
	}
	if havingPredicate != nil {
		queryBuilder.HavingPred(havingPredicate)
	}

	if pagePageSize != nil {
		err := validatePagePageSize(pagePageSize)
		if err != nil {
			twrapper.SetHasErrors(true)
			twrapper.AddError(err.Error())
			ThrowException(cErrors.Cause(err))
			return twrapper
		}

		if pagePageSize[0] > 0 {
			queryArguments.AddQueryArgument(":num_of_records", pagePageSize[1])
			queryArguments.AddQueryArgument(":offset", (pagePageSize[0]-1)*pagePageSize[1])
			queryBuilder.Limit()
		}
	}

	twrapper = selectData(organizationId, queryBuilder.ToString(), queryArguments)

	if pagePageSize != nil {
		if pagePageSize[0] > 0 {
			twrapper.SetData(paginate(tableName, twrapper.GetData(),
				CountWhereGroupBy(organizationId, tableName, wherePredicate, groupByColumns, havingPredicate, queryArguments),
				pagePageSize[0], pagePageSize[1],
			))
		}
	}
	return twrapper
}

func SelectWhereGroupByOrderBy(organizationId, tableName, columns string,
	wherePredicate *FilterPredicate,
	groupByColumns string, havingPredicate *FilterPredicate,
	columnOrderBy string,
	queryArguments *CypressHashMap,
	pagePageSize []int) (twrapper *TransactionWrapper) {

	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	twrapper = NewTransactionWrapper()
	if queryArguments == nil {
		queryArguments = NewMap()
	}
	queryArguments.SetTableName(tableName)

	queryBuilder := NewQueryBuilder()
	queryBuilder.Select()
	if columns != "" {
		queryBuilder.SelectColumn(columns)
	}

	queryBuilder.FromTable(tableName)

	if wherePredicate != nil && wherePredicate.GetClause() != "" {
		queryBuilder.WherePred(wherePredicate)
	}

	if groupByColumns != "" {
		queryBuilder.GroupBy(groupByColumns)
	}

	if havingPredicate != nil {
		queryBuilder.HavingPred(havingPredicate)
	}

	if columnOrderBy != "" {
		queryBuilder.OrderBy(columnOrderBy)
	}

	if pagePageSize != nil {
		err := validatePagePageSize(pagePageSize)
		if err != nil {
			twrapper.SetHasErrors(true)
			twrapper.AddError(err.Error())
			ThrowException(cErrors.Cause(err))
			return twrapper
		}

		if pagePageSize[0] > 0 {
			queryArguments.AddQueryArgument(":num_of_records", pagePageSize[1])
			queryArguments.AddQueryArgument(":offset", (pagePageSize[0]-1)*pagePageSize[1])
			queryBuilder.Limit()
		}
	}

	twrapper = selectData(organizationId, queryBuilder.ToString(), queryArguments)

	if pagePageSize != nil {
		if pagePageSize[0] > 0 {
			twrapper.SetData(paginate(tableName, twrapper.GetData(),
				CountWhereGroupBy(organizationId, tableName, wherePredicate, groupByColumns, havingPredicate, queryArguments),
				pagePageSize[0], pagePageSize[1],
			))
		}
	}
	return twrapper
}

func CountWhereGroupBy(organizationId, tableName string, wherePredicate *FilterPredicate, groupByColumns string, havingPredicate *FilterPredicate, queryArguments *CypressHashMap) int {
	if queryArguments == nil {
		queryArguments = NewMap()
	}
	queryArguments.SetTableName(tableName)

	countQueryBuilder := NewQueryBuilder().Select().SelectColumn("COUNT(*) AS count")
	countQueryBuilder.FromTable(tableName)

	if wherePredicate != nil && wherePredicate.GetClause() != "" {
		countQueryBuilder.WherePred(wherePredicate)
	}

	if groupByColumns != "" {
		countQueryBuilder.GroupBy(groupByColumns)
	}
	if havingPredicate != nil {
		countQueryBuilder.HavingPred(havingPredicate)
	}

	twrapper := selectData(organizationId, countQueryBuilder.ToString(), queryArguments)
	cypressList := twrapper.GetData().(*CypressArrayList)
	hashMap := cypressList.GetRecord(0)
	count, _ := strconv.Atoi(hashMap.GetStringValue("count"))
	return count
}

func paginate(tableName string, records interface{}, totalCount, page, pageSize int) *PageableWrapper {
	wrapper := NewPageableWrapper()

	wrapper.SetCurrentPage(page)
	wrapper.SetPageSize(pageSize)
	wrapper.SetDomain(tableName)
	wrapper.SetData(records)
	wrapper.SetTotalCount(totalCount)

	lastPage := int(math.Ceil(float64(totalCount) / float64(pageSize)))

	if lastPage == 0 {
		lastPage = 1
	}
	wrapper.SetLastPage(lastPage)
	return wrapper
}
