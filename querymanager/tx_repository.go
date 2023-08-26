package querymanager

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/codecypress/go-ancillary-utils/cypressutils"
	"github.com/codecypress/go-ancillary-utils/exceptions"
	cErrors "github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"runtime/debug"
)

type TxRepository struct {
	dbConn         *sql.DB
	tx             *sql.Tx
	organizationId string
}

func NewTxRepository(organizationId string) (*TxRepository, error) {
	txRepository := &TxRepository{organizationId: organizationId}

	dbConn, err := GetConnection(organizationId)
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return nil, err
	}

	tx, err := dbConn.Begin()
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return nil, err
	}

	txRepository.dbConn = dbConn
	txRepository.tx = tx
	return txRepository, nil
}

func (txRepository *TxRepository) Close() {
	if txRepository.dbConn != nil {
		txRepository.dbConn.Close()
	}

	txRepository.tx = nil
	txRepository.dbConn = nil
}

func (txRepository *TxRepository) Commit() error {

	defer txRepository.Close()

	err := txRepository.tx.Commit()
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return err
	}
	return nil
}

func (txRepository *TxRepository) Rollback() error {
	defer txRepository.Close()

	err := txRepository.tx.Rollback()
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return err
	}

	return nil
}

func (txRepository *TxRepository) GetTx() *sql.Tx {
	return txRepository.tx
}

func (txRepository *TxRepository) GetDbConn() *sql.DB {
	return txRepository.dbConn
}

func (txRepository *TxRepository) TxInsert(organizationId, tableName string, recordHashMap *cypressutils.CypressHashMap) (twrapper *cypressutils.TransactionWrapper, err error) {
	queryArguments := cypressutils.NewMap()

	for pair := recordHashMap.GetData().Oldest(); pair != nil; pair = pair.Next() {
		field := fmt.Sprintf("%v", pair.Key)
		queryArguments.PutValue(":"+field, pair.Value)
	}

	queryBuilder := NewQueryBuilder()
	queryBuilder.Insert().Into(tableName).Columns(queryArguments.GetKeysNoStartColon()).Values(queryArguments.GetKeysWithStartColon())

	return txInsert(organizationId, txRepository.tx, queryBuilder, queryArguments)
}

func (txRepository *TxRepository) TxInsertOnDuplicate(organizationId, tableName string, recordHashMap *cypressutils.CypressHashMap, onDuplicateColumns []string) (twrapper *cypressutils.TransactionWrapper, err error) {
	queryArguments := cypressutils.NewMap()

	for pair := recordHashMap.GetData().Oldest(); pair != nil; pair = pair.Next() {
		field := fmt.Sprintf("%v", pair.Key)
		queryArguments.PutValue(":"+field, pair.Value)
	}

	queryBuilder := NewQueryBuilder()
	queryBuilder.Insert().Into(tableName).Columns(queryArguments.GetKeysNoStartColon()).Values(queryArguments.GetKeysWithStartColon())
	if onDuplicateColumns != nil {
		queryBuilder.OnDuplicateKey(onDuplicateColumns)
	}

	return txInsert(organizationId, txRepository.tx, queryBuilder, queryArguments)
}

func (txRepository *TxRepository) TxBatchInsert(organizationId, tableName string, queryArgsList *cypressutils.CypressArrayList) (twrapper *cypressutils.TransactionWrapper, err error) {
	queryBuilder := NewQueryBuilder()
	queryBuilder.Insert().Into(tableName).Columns(queryArgsList.GetRecord(0).GetKeysNoStartColon())
	return txBatchInsert(organizationId, txRepository.tx, queryBuilder, queryArgsList)
}

func (txRepository *TxRepository) TxGetPrimaryKeyColumns(organizationId string, tableName string) (twrapper *cypressutils.TransactionWrapper, err error) {
	twrapper = cypressutils.NewTransactionWrapper()

	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	list, err := txGetPrimaryKeyColumns(organizationId, txRepository.tx, tableName)
	if err != nil {
		twrapper.SetHasErrors(true)
		twrapper.AddError(err.Error())
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	twrapper.SetData(list)

	return twrapper, nil
}

func (txRepository *TxRepository) TxUpdate(organizationId,
	tableName string,
	updateSet *cypressutils.CypressHashMap,
	filterPredicate *FilterPredicate,
	queryArguments *cypressutils.CypressHashMap, selectPreUpdate bool, pagePageSize []int) (twrapper *cypressutils.TransactionWrapper, err error) {

	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	twrapper = cypressutils.NewTransactionWrapper()
	if queryArguments == nil {
		queryArguments = cypressutils.NewMap()
	}
	queryArguments.SetTableName(tableName)

	if updateSet == nil || updateSet.IsEmpty() {
		twrapper.SetHasErrors(true)
		twrapper.AddError("UPDATE: Update set should not be empty")
		logrus.Error(errors.New("UPDATE: Update set should not be empty"))
		return twrapper, err
	}

	updateSetVariables := cypressutils.NewMap()
	for pair := updateSet.GetData().Oldest(); pair != nil; pair = pair.Next() {
		field := fmt.Sprintf("%v", pair.Key)
		updateSetVariables.PutValue(field, ":"+field)
		queryArguments.PutValue(":"+field, pair.Value)
	}

	if selectPreUpdate {

		primaryKeyColsList, err := _txGetPrimaryKeyColumns_(organizationId, txRepository.tx, tableName)
		if err != nil {
			twrapper.SetHasErrors(true)
			twrapper.AddError(err.Error())
			return twrapper, err
		}

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
				exceptions.ThrowException(cErrors.Cause(err))
				return twrapper, err
			}

			if pagePageSize[0] > 0 {
				queryArguments.AddQueryArgument(":num_of_records", pagePageSize[1])
				queryArguments.AddQueryArgument(":offset", (pagePageSize[0]-1)*pagePageSize[1])
				queryBuilder.Limit()
			}
		}

		return txUpdate(organizationId, txRepository.tx, queryBuilder, queryArguments)
	}

	queryBuilder := NewQueryBuilder()
	queryBuilder.Update(tableName).Set(updateSetVariables)

	if filterPredicate != nil && filterPredicate.GetClause() != "" {
		queryBuilder.WherePred(filterPredicate)
	}

	return txUpdate(organizationId, txRepository.tx, queryBuilder, queryArguments)
}

func (txRepository *TxRepository) TxDelete(organizationId, tableName string, filterPredicate *FilterPredicate, queryArguments *cypressutils.CypressHashMap) (twrapper *cypressutils.TransactionWrapper, err error) {
	twrapper = cypressutils.NewTransactionWrapper()
	if queryArguments == nil {
		queryArguments = cypressutils.NewMap()
	}
	queryArguments.SetTableName(tableName)

	queryBuilder := NewQueryBuilder()
	queryBuilder.DeleteFrom(tableName)

	if filterPredicate != nil && filterPredicate.GetClause() != "" {
		queryBuilder.WherePred(filterPredicate)
	}

	return txDelete(organizationId, txRepository.tx, queryBuilder, queryArguments)
}

func (txRepository *TxRepository) TxJoinSelectQuery(organizationId string,
	queryBuilder *QueryBuilder,
	queryArguments *cypressutils.CypressHashMap,
	pagePageSize []int) (twrapper *cypressutils.TransactionWrapper, err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	twrapper = cypressutils.NewTransactionWrapper()
	tableName := queryBuilder.GetTableName()
	if queryArguments == nil {
		queryArguments = cypressutils.NewMap()
	}
	queryArguments.SetTableName(tableName)

	qrQueryBuilder := NewQueryBuilder().RawQuery(queryBuilder.ToString())
	if pagePageSize != nil {
		err := validatePagePageSize(pagePageSize)
		if err != nil {
			twrapper.SetHasErrors(true)
			twrapper.AddError(err.Error())
			exceptions.ThrowException(cErrors.Cause(err))
			return twrapper, err
		}

		if pagePageSize[0] > 0 {
			queryArguments.AddQueryArgument(":num_of_records", pagePageSize[1])
			queryArguments.AddQueryArgument(":offset", (pagePageSize[0]-1)*pagePageSize[1])
			qrQueryBuilder.Limit()
		}
	}

	twrapper, err = txSelectData(organizationId, txRepository.tx, qrQueryBuilder.ToString(), queryArguments)
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	if pagePageSize != nil {
		if pagePageSize[0] > 0 {
			result, err := txRepository.TxJoinCountQuery(organizationId, queryBuilder, queryArguments)
			if err != nil {
				twrapper.SetHasErrors(true)
				twrapper.AddError(err.Error())
				return twrapper, err
			}

			twrapper.SetData(paginate(tableName, twrapper.GetData(), result, pagePageSize[0], pagePageSize[1]))
		}
	}
	return twrapper, nil
}

func (txRepository *TxRepository) TxJoinCountQuery(organizationId string, queryBuilder *QueryBuilder, queryArguments *cypressutils.CypressHashMap) (int, error) {
	countQueryBuilder := NewQueryBuilder().Select().SelectColumn("COUNT(*) AS count")
	countQueryBuilder.From()
	countQueryBuilder.JoinPhrase(queryBuilder.GetJoinStatement())

	if queryBuilder.GetWhereClause() != "" {
		countQueryBuilder.WhereStr(queryBuilder.GetWhereClause())
	}

	twrapper, err := txSelectData(organizationId, txRepository.tx, countQueryBuilder.ToString(), queryArguments)
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return 0, err
	}

	cypressList := twrapper.GetData().(*cypressutils.CypressArrayList)
	hashMap := cypressList.GetRecord(0)
	return hashMap.GetValue("count").(int), nil
}

func (txRepository *TxRepository) TxJoinExistsQuery(organizationId string, queryBuilder *QueryBuilder, queryArguments *cypressutils.CypressHashMap) (bool, error) {
	countQueryBuilder := NewQueryBuilder().Select().SelectColumn("1")
	countQueryBuilder.From()
	countQueryBuilder.JoinPhrase(queryBuilder.GetJoinStatement())
	countQueryBuilder.WhereStr(queryBuilder.GetWhereClause())

	twrapper, err := txSelectData(organizationId, txRepository.tx, countQueryBuilder.ToString(), queryArguments)
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return false, err
	}

	cypressList := twrapper.GetData().(*cypressutils.CypressArrayList)
	return !(cypressList == nil || cypressList.Size() < 1), nil
}

func (txRepository *TxRepository) TxSelectWithQueryBuilder(organizationId string,
	queryBuilder *QueryBuilder,
	queryArguments *cypressutils.CypressHashMap,
	pagePageSize []int) (twrapper *cypressutils.TransactionWrapper, err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	twrapper = cypressutils.NewTransactionWrapper()
	tableName := queryBuilder.GetTableName()
	if queryArguments == nil {
		queryArguments = cypressutils.NewMap()
	}
	queryArguments.SetTableName(tableName)

	qrQueryBuilder := NewQueryBuilder().RawQuery(queryBuilder.ToString())

	if pagePageSize != nil {
		err := validatePagePageSize(pagePageSize)
		if err != nil {
			twrapper.SetHasErrors(true)
			twrapper.AddError(err.Error())
			exceptions.ThrowException(cErrors.Cause(err))
			return twrapper, err
		}

		if pagePageSize[0] > 0 {
			queryArguments.AddQueryArgument(":num_of_records", pagePageSize[1])
			queryArguments.AddQueryArgument(":offset", (pagePageSize[0]-1)*pagePageSize[1])
			qrQueryBuilder.Limit()
		}
	}

	twrapper, err = txSelectData(organizationId, txRepository.tx, qrQueryBuilder.ToString(), queryArguments)
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	if pagePageSize != nil {
		if pagePageSize[0] > 0 {
			result, err := txRepository.TxCount(organizationId, queryBuilder, queryArguments)
			if err != nil {
				twrapper.SetHasErrors(true)
				twrapper.AddError(err.Error())
				return twrapper, err
			}
			twrapper.SetData(paginate(tableName, twrapper.GetData(), result, pagePageSize[0], pagePageSize[1]))
		}
	}

	return twrapper, nil
}

func (txRepository *TxRepository) TxCount(organizationId string, queryBuilder *QueryBuilder, queryArguments *cypressutils.CypressHashMap) (int, error) {
	countQueryBuilder := NewQueryBuilder().Select().SelectColumn("COUNT(*) AS count")
	countQueryBuilder.FromTable(queryBuilder.GetTableName())
	if queryBuilder.GetWhereClause() != "" {
		countQueryBuilder.WhereStr(queryBuilder.GetWhereClause())
	}

	twrapper, err := txSelectData(organizationId, txRepository.tx, countQueryBuilder.ToString(), queryArguments)
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return 0, err
	}
	cypressList := twrapper.GetData().(*cypressutils.CypressArrayList)
	hashMap := cypressList.GetRecord(0)
	return hashMap.GetValue("count").(int), nil
}

func (txRepository *TxRepository) TxExists(organizationId string, queryBuilder *QueryBuilder, queryArguments *cypressutils.CypressHashMap) (bool, error) {
	countQueryBuilder := NewQueryBuilder().Select().SelectColumn("1")
	countQueryBuilder.FromTable(queryBuilder.GetTableName())
	countQueryBuilder.WhereStr(queryBuilder.GetWhereClause())

	twrapper, err := txSelectData(organizationId, txRepository.tx, countQueryBuilder.ToString(), queryArguments)
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return false, err
	}

	cypressList := twrapper.GetData().(*cypressutils.CypressArrayList)
	return !(cypressList == nil || cypressList.Size() < 1), nil
}

func (txRepository *TxRepository) TxSelect(organizationId, tableName, columns string, pagePageSize []int) (twrapper *cypressutils.TransactionWrapper, err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	twrapper = cypressutils.NewTransactionWrapper()
	queryArguments := cypressutils.NewMap()

	queryBuilder := NewQueryBuilder().Select()
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
			exceptions.ThrowException(cErrors.Cause(err))
			return twrapper, err
		}

		if pagePageSize[0] > 0 {
			queryArguments.AddQueryArgument(":num_of_records", pagePageSize[1])
			queryArguments.AddQueryArgument(":offset", (pagePageSize[0]-1)*pagePageSize[1])
			queryBuilder.Limit()
		}
	}

	twrapper, err = txSelectData(organizationId, txRepository.tx, queryBuilder.ToString(), queryArguments)
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	if pagePageSize != nil {
		if pagePageSize[0] > 0 {
			result, err := txRepository.TxCount(organizationId, queryBuilder, queryArguments)
			if err != nil {
				twrapper.SetHasErrors(true)
				twrapper.AddError(err.Error())
				return twrapper, err
			}
			twrapper.SetData(paginate(tableName, twrapper.GetData(), result, pagePageSize[0], pagePageSize[1]))
		}
	}
	return twrapper, nil
}

func (txRepository *TxRepository) TxSelectOrderBy(organizationId, tableName, columns, columnOrderBy string, pagePageSize []int) (twrapper *cypressutils.TransactionWrapper, err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	twrapper = cypressutils.NewTransactionWrapper()
	queryArguments := cypressutils.NewMap()

	queryBuilder := NewQueryBuilder().Select()
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
			exceptions.ThrowException(cErrors.Cause(err))
			return twrapper, err
		}

		if pagePageSize[0] > 0 {
			queryArguments.AddQueryArgument(":num_of_records", pagePageSize[1])
			queryArguments.AddQueryArgument(":offset", (pagePageSize[0]-1)*pagePageSize[1])
			queryBuilder.Limit()
		}
	}

	twrapper, err = txSelectData(organizationId, txRepository.tx, queryBuilder.ToString(), queryArguments)
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	if pagePageSize != nil {
		if pagePageSize[0] > 0 {
			result, err := txRepository.TxCount(organizationId, queryBuilder, queryArguments)

			if err != nil {
				twrapper.SetHasErrors(true)
				twrapper.AddError(err.Error())
				return twrapper, err
			}

			twrapper.SetData(paginate(tableName, twrapper.GetData(), result, pagePageSize[0], pagePageSize[1]))
		}
	}
	return twrapper, nil
}

func (txRepository *TxRepository) TxSelectGroupBy(organizationId,
	tableName,
	columns,
	groupByColumns string,
	havingPredicate *FilterPredicate,
	queryArguments *cypressutils.CypressHashMap,
	pagePageSize []int) (twrapper *cypressutils.TransactionWrapper, err error) {

	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	twrapper = cypressutils.NewTransactionWrapper()
	if queryArguments == nil {
		queryArguments = cypressutils.NewMap()
	}
	queryArguments.SetTableName(tableName)

	queryBuilder := NewQueryBuilder().Select()
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
			exceptions.ThrowException(cErrors.Cause(err))
			return twrapper, err
		}

		if pagePageSize[0] > 0 {
			queryArguments.AddQueryArgument(":num_of_records", pagePageSize[1])
			queryArguments.AddQueryArgument(":offset", (pagePageSize[0]-1)*pagePageSize[1])
			queryBuilder.Limit()
		}
	}

	twrapper, err = txSelectData(organizationId, txRepository.tx, queryBuilder.ToString(), queryArguments)
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	if pagePageSize != nil {
		if pagePageSize[0] > 0 {
			result, err := txRepository.TxCountGroupBy(organizationId, tableName, groupByColumns, havingPredicate, queryArguments)
			if err != nil {
				twrapper.SetHasErrors(true)
				twrapper.AddError(err.Error())
				return twrapper, err
			}

			twrapper.SetData(paginate(tableName, twrapper.GetData(), result, pagePageSize[0], pagePageSize[1]))
		}
	}
	return twrapper, nil
}

func (txRepository *TxRepository) TxSelectGroupByOrderBy(organizationId,
	tableName,
	columns,
	groupByColumns string,
	havingPredicate *FilterPredicate,
	columnOrderBy string,
	queryArguments *cypressutils.CypressHashMap,
	pagePageSize []int) (twrapper *cypressutils.TransactionWrapper, err error) {

	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	twrapper = cypressutils.NewTransactionWrapper()
	if queryArguments == nil {
		queryArguments = cypressutils.NewMap()
	}
	queryArguments.SetTableName(tableName)

	queryBuilder := NewQueryBuilder().Select()
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
			exceptions.ThrowException(cErrors.Cause(err))
			return twrapper, err
		}

		if pagePageSize[0] > 0 {
			queryArguments.AddQueryArgument(":num_of_records", pagePageSize[1])
			queryArguments.AddQueryArgument(":offset", (pagePageSize[0]-1)*pagePageSize[1])
			queryBuilder.Limit()
		}
	}

	twrapper, err = txSelectData(organizationId, txRepository.tx, queryBuilder.ToString(), queryArguments)
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	if pagePageSize != nil {
		if pagePageSize[0] > 0 {
			result, err := txRepository.TxCountGroupBy(organizationId, tableName, groupByColumns, havingPredicate, queryArguments)
			if err != nil {
				twrapper.SetHasErrors(true)
				twrapper.AddError(err.Error())
				return twrapper, err
			}

			twrapper.SetData(paginate(tableName, twrapper.GetData(), result, pagePageSize[0], pagePageSize[1]))
		}
	}
	return twrapper, nil
}

func (txRepository *TxRepository) TxCountGroupBy(organizationId, tableName, groupByColumns string, havingPredicate *FilterPredicate, queryArguments *cypressutils.CypressHashMap) (int, error) {
	if queryArguments == nil {
		queryArguments = cypressutils.NewMap()
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

	twrapper, err := txSelectData(organizationId, txRepository.tx, countQueryBuilder.ToString(), queryArguments)
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return 0, err
	}

	cypressList := twrapper.GetData().(*cypressutils.CypressArrayList)
	hashMap := cypressList.GetRecord(0)
	return hashMap.GetValue("count").(int), nil
}

func (txRepository *TxRepository) TxSelectWhere(organizationId, tableName, columns string, filterPredicate *FilterPredicate, queryArguments *cypressutils.CypressHashMap, pagePageSize []int) (twrapper *cypressutils.TransactionWrapper, err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	twrapper = cypressutils.NewTransactionWrapper()
	if queryArguments == nil {
		queryArguments = cypressutils.NewMap()
	}
	queryArguments.SetTableName(tableName)

	queryBuilder := NewQueryBuilder().Select()
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
			exceptions.ThrowException(cErrors.Cause(err))
			return twrapper, err
		}

		if pagePageSize[0] > 0 {
			queryArguments.AddQueryArgument(":num_of_records", pagePageSize[1])
			queryArguments.AddQueryArgument(":offset", (pagePageSize[0]-1)*pagePageSize[1])
			queryBuilder.Limit()
		}
	}

	twrapper, err = txSelectData(organizationId, txRepository.tx, queryBuilder.ToString(), queryArguments)
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	if pagePageSize != nil {
		if pagePageSize[0] > 0 {
			result, err := txRepository.TxCountWhere(organizationId, tableName, filterPredicate, queryArguments)
			if err != nil {
				twrapper.SetHasErrors(true)
				twrapper.AddError(err.Error())
				return twrapper, err
			}
			twrapper.SetData(paginate(tableName, twrapper.GetData(), result, pagePageSize[0], pagePageSize[1]))
		}
	}
	return twrapper, nil
}

func (txRepository *TxRepository) TxCountWhere(organizationId, tableName string, filterPredicate *FilterPredicate, queryArguments *cypressutils.CypressHashMap) (int, error) {
	if queryArguments == nil {
		queryArguments = cypressutils.NewMap()
	}
	queryArguments.SetTableName(tableName)

	countQueryBuilder := NewQueryBuilder().Select().SelectColumn("COUNT(*) AS count")
	countQueryBuilder.FromTable(tableName)

	if filterPredicate != nil && filterPredicate.GetClause() != "" {
		countQueryBuilder.WherePred(filterPredicate)
	}

	twrapper, err := txSelectData(organizationId, txRepository.tx, countQueryBuilder.ToString(), queryArguments)

	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return 0, err
	}

	cypressList := twrapper.GetData().(*cypressutils.CypressArrayList)
	hashMap := cypressList.GetRecord(0)
	return hashMap.GetValue("count").(int), nil
}

func (txRepository *TxRepository) TxSelectWhereOrderBy(organizationId, tableName, columns string,
	filterPredicate *FilterPredicate,
	columnOrderBy string,
	queryArguments *cypressutils.CypressHashMap,
	pagePageSize []int) (twrapper *cypressutils.TransactionWrapper, err error) {

	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	twrapper = cypressutils.NewTransactionWrapper()
	if queryArguments == nil {
		queryArguments = cypressutils.NewMap()
	}
	queryArguments.SetTableName(tableName)

	queryBuilder := NewQueryBuilder().Select()
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
			exceptions.ThrowException(cErrors.Cause(err))
			return twrapper, err
		}

		if pagePageSize[0] > 0 {
			queryArguments.AddQueryArgument(":num_of_records", pagePageSize[1])
			queryArguments.AddQueryArgument(":offset", (pagePageSize[0]-1)*pagePageSize[1])
			queryBuilder.Limit()
		}
	}

	twrapper, err = txSelectData(organizationId, txRepository.tx, queryBuilder.ToString(), queryArguments)
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	if pagePageSize != nil {
		if pagePageSize[0] > 0 {
			result, err := txRepository.TxCountWhere(organizationId, tableName, filterPredicate, queryArguments)
			if err != nil {
				twrapper.SetHasErrors(true)
				twrapper.AddError(err.Error())
				return twrapper, err
			}

			twrapper.SetData(paginate(tableName, twrapper.GetData(), result, pagePageSize[0], pagePageSize[1]))
		}
	}
	return twrapper, nil
}

func (txRepository *TxRepository) TxSelectWhereGroupBy(organizationId, tableName, columns string,
	wherePredicate *FilterPredicate,
	groupByColumns string, havingPredicate *FilterPredicate,
	queryArguments *cypressutils.CypressHashMap,
	pagePageSize []int) (twrapper *cypressutils.TransactionWrapper, err error) {

	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	twrapper = cypressutils.NewTransactionWrapper()
	if queryArguments == nil {
		queryArguments = cypressutils.NewMap()
	}
	queryArguments.SetTableName(tableName)

	queryBuilder := NewQueryBuilder().Select()
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
			if err != nil {
				twrapper.SetHasErrors(true)
				twrapper.AddError(err.Error())
				return twrapper, err
			}
		}

		if pagePageSize[0] > 0 {
			queryArguments.AddQueryArgument(":num_of_records", pagePageSize[1])
			queryArguments.AddQueryArgument(":offset", (pagePageSize[0]-1)*pagePageSize[1])
			queryBuilder.Limit()
		}
	}

	twrapper, err = txSelectData(organizationId, txRepository.tx, queryBuilder.ToString(), queryArguments)
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	if pagePageSize != nil {
		if pagePageSize[0] > 0 {
			result, err := txRepository.TxCountWhereGroupBy(organizationId, tableName, wherePredicate, groupByColumns, havingPredicate, queryArguments)
			if err != nil {
				twrapper.SetHasErrors(true)
				twrapper.AddError(err.Error())
				return twrapper, err
			}
			twrapper.SetData(paginate(tableName, twrapper.GetData(), result, pagePageSize[0], pagePageSize[1]))
		}
	}
	return twrapper, nil
}

func (txRepository *TxRepository) TxSelectWhereGroupByOrderBy(organizationId, tableName, columns string,
	wherePredicate *FilterPredicate,
	groupByColumns string, havingPredicate *FilterPredicate,
	columnOrderBy string,
	queryArguments *cypressutils.CypressHashMap,
	pagePageSize []int) (twrapper *cypressutils.TransactionWrapper, err error) {

	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	twrapper = cypressutils.NewTransactionWrapper()
	if queryArguments == nil {
		queryArguments = cypressutils.NewMap()
	}
	queryArguments.SetTableName(tableName)

	queryBuilder := NewQueryBuilder().Select()
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
			exceptions.ThrowException(cErrors.Cause(err))
			return twrapper, err
		}

		if pagePageSize[0] > 0 {
			queryArguments.AddQueryArgument(":num_of_records", pagePageSize[1])
			queryArguments.AddQueryArgument(":offset", (pagePageSize[0]-1)*pagePageSize[1])
			queryBuilder.Limit()
		}
	}

	twrapper, err = txSelectData(organizationId, txRepository.tx, queryBuilder.ToString(), queryArguments)
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return twrapper, err
	}

	if pagePageSize != nil {
		if pagePageSize[0] > 0 {

			result, err := txRepository.TxCountWhereGroupBy(organizationId, tableName, wherePredicate, groupByColumns, havingPredicate, queryArguments)
			if err != nil {
				twrapper.SetHasErrors(true)
				twrapper.AddError(err.Error())
				return twrapper, err
			}

			twrapper.SetData(paginate(tableName, twrapper.GetData(), result, pagePageSize[0], pagePageSize[1]))
		}
	}
	return twrapper, nil
}

func (txRepository *TxRepository) TxCountWhereGroupBy(organizationId, tableName string, wherePredicate *FilterPredicate, groupByColumns string, havingPredicate *FilterPredicate, queryArguments *cypressutils.CypressHashMap) (int, error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			debug.PrintStack()
		}
	}()

	if queryArguments == nil {
		queryArguments = cypressutils.NewMap()
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

	twrapper, err := txSelectData(organizationId, txRepository.tx, countQueryBuilder.ToString(), queryArguments)

	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return 0, err
	}

	cypressList := twrapper.GetData().(*cypressutils.CypressArrayList)
	hashMap := cypressList.GetRecord(0)
	return hashMap.GetValue("count").(int), nil
}
