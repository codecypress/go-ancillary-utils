package cypressutils

import (
	"github.com/codecypress/go-ancillary-utils/exceptions"
	"github.com/codecypress/go-ancillary-utils/logging"
	"github.com/codecypress/go-ancillary-utils/querymanager"
	cErrors "github.com/pkg/errors"
	"strings"
)

type TransactionWrapper struct {
	HasErrors         bool        `json:"has_errors"`
	HasWarnings       bool        `json:"has_warnings"`
	StatusCode        int         `json:"status_code"`
	Errors            []string    `json:"errors"`
	Messages          []string    `json:"messages"`
	Warnings          []string    `json:"warnings"`
	ErrorsStackTrace  []string    `json:"errors_stack_trace"`
	QueryExecutedList []string    `json:"query_executed_list"`
	Data              interface{} `json:"data"`
}

func NewTransactionWrapper(data ...bool) *TransactionWrapper {
	tempData := false
	if data != nil {
		tempData = data[0]
	}

	return &TransactionWrapper{
		HasErrors:         false,
		HasWarnings:       false,
		Errors:            []string{},
		Messages:          []string{},
		Warnings:          []string{},
		ErrorsStackTrace:  []string{},
		QueryExecutedList: []string{},
		Data:              tempData,
	}
}

func (wrapper *TransactionWrapper) CopyFrom(otherWrapper *TransactionWrapper) {
	wrapper.HasErrors = otherWrapper.HasErrors
	wrapper.HasWarnings = otherWrapper.HasWarnings
	for _, str := range otherWrapper.Errors {
		wrapper.Errors = append(wrapper.Errors, str)
	}
	for _, str := range otherWrapper.Messages {
		wrapper.Messages = append(wrapper.Messages, str)
	}
	for _, str := range otherWrapper.Warnings {
		wrapper.Warnings = append(wrapper.Warnings, str)
	}
	for _, str := range otherWrapper.ErrorsStackTrace {
		wrapper.ErrorsStackTrace = append(wrapper.ErrorsStackTrace, str)
	}
	for _, str := range otherWrapper.QueryExecutedList {
		wrapper.QueryExecutedList = append(wrapper.QueryExecutedList, str)
	}
}

func (wrapper *TransactionWrapper) SetHasErrors(hasErrors bool) {
	wrapper.HasErrors = hasErrors
}

func (wrapper *TransactionWrapper) SetHasWarnings(hasWarnings bool) {
	wrapper.HasWarnings = hasWarnings
}

func (wrapper *TransactionWrapper) GetData() interface{} {
	return wrapper.Data
}

func (wrapper *TransactionWrapper) SetData(data interface{}) {
	wrapper.Data = data
}

func (wrapper *TransactionWrapper) AddError(error string) {
	wrapper.Errors = append(wrapper.Errors, error)
}

func (wrapper *TransactionWrapper) AddMessage(message string) {
	wrapper.Messages = append(wrapper.Messages, message)
}

func (wrapper *TransactionWrapper) AddWarning(warning string) {
	wrapper.Warnings = append(wrapper.Warnings, warning)
}

func (wrapper *TransactionWrapper) AddErrorStackTrace(errorStackTrace string) {
	wrapper.ErrorsStackTrace = append(wrapper.ErrorsStackTrace, errorStackTrace)
}

func (wrapper *TransactionWrapper) GetStatusCode() int {
	return wrapper.StatusCode
}

func (wrapper *TransactionWrapper) SetStatusCode(statusCode int) {
	wrapper.StatusCode = statusCode
}

func (wrapper *TransactionWrapper) GetErrors(delims ...string) string {
	delimiter := "\n"
	if delims != nil {
		delimiter = delims[0]
	}
	return strings.Join(wrapper.Errors, delimiter)
}

func (wrapper *TransactionWrapper) GetErrorsAsList() []string {
	return wrapper.Errors
}

func (wrapper *TransactionWrapper) GetMessages(delims ...string) string {
	delimiter := "\n"
	if delims != nil {
		delimiter = delims[0]
	}
	return strings.Join(wrapper.Messages, delimiter)
}

func (wrapper *TransactionWrapper) GetMessagesList() []string {
	return wrapper.Messages
}

func (wrapper *TransactionWrapper) GetErrorStackTrace(delims ...string) string {
	delimiter := "\n"
	if delims != nil {
		delimiter = delims[0]
	}
	return strings.Join(wrapper.ErrorsStackTrace, delimiter)
}

func (wrapper *TransactionWrapper) GetErrorStackTraceAsList() []string {
	return wrapper.ErrorsStackTrace
}

func (wrapper *TransactionWrapper) GetQueriesExecuted() []string {
	return wrapper.QueryExecutedList
}

func (wrapper *TransactionWrapper) AddQueryExecuted(queryExecuted string) {
	wrapper.QueryExecutedList = append(wrapper.QueryExecutedList, queryExecuted)
}

func (wrapper *TransactionWrapper) SetErrorsStackTrace(queryExecutedList []string) {
	wrapper.QueryExecutedList = queryExecutedList
}

func (wrapper *TransactionWrapper) SetErrors(errors []string) {
	wrapper.Errors = errors
}

func (wrapper *TransactionWrapper) SetMessages(messages []string) {
	wrapper.Messages = messages
}

func (wrapper *TransactionWrapper) DisplayQueriesExecuted() {
	for _, query := range wrapper.QueryExecutedList {
		logging.Println(querymanager.FormatSQL(query))
	}
}

func (wrapper *TransactionWrapper) GetSingleRecord() (result *CypressHashMap, err error) {

	//interface{}.(records).(type)
	if wrapper.Data == nil {
		return nil, nil
	}

	switch wrapper.Data.(type) {
	case *CypressArrayList:
		{
			list := wrapper.Data.(*CypressArrayList)
			return list.GetRecord(0), nil
		}
	case *CypressHashMap:
		{
			return wrapper.Data.(*CypressHashMap), nil
		}
	case *PageableWrapper:
		{
			return wrapper.Data.(*PageableWrapper).GetSingleRecord()
		}
	default:
		err = cErrors.New("failed to call 'getSingleRecord()'. Can only be called if Data is of type CypressHashMap, CypressArrayList or PageableWrapper")
		exceptions.ThrowException(err)
		return nil, err
	}
}
