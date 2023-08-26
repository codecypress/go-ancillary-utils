package cypressutils

import (
	"bytes"
	"fmt"
)

type NamedParameterQuery struct {
	parameters     []interface{}
	queryArguments *CypressHashMap
	originalQuery  string
	parsedQuery    string
}

func NewNamedParameterQuery(sqlQuery string, queryArguments *CypressHashMap) *NamedParameterQuery {
	temp := &NamedParameterQuery{
		originalQuery:  sqlQuery,
		queryArguments: queryArguments,
	}

	temp.parseSQLQuery()
	return temp
}

func (namedParameterQuery *NamedParameterQuery) parseSQLQuery() {
	length := len(namedParameterQuery.originalQuery)

	var parsedQuery bytes.Buffer

	inSingleQuote := false
	inDoubleQuote := false
	inSingleLineComment := false
	inMultiLineComment := false

	index := 1
	for i := 0; i < length; i++ {
		c := namedParameterQuery.originalQuery[i]
		if inSingleQuote {
			if c == '\'' {
				inSingleQuote = false
			}
		} else if inDoubleQuote {
			if c == '"' {
				inDoubleQuote = false
			}
		} else if inMultiLineComment {
			if c == '*' && namedParameterQuery.originalQuery[i+1] == '/' {
				inMultiLineComment = false
			}
		} else if inSingleLineComment {
			if c == '\n' {
				inSingleLineComment = false
			}
		} else if c == '\'' {
			inSingleQuote = true
		} else if c == '"' {
			inDoubleQuote = true
		} else if c == '/' && namedParameterQuery.originalQuery[i+1] == '*' {
			inMultiLineComment = true
		} else if c == '-' && namedParameterQuery.originalQuery[i+1] == '-' {
			inSingleLineComment = true
		} else if c == ':' && i+1 < length && IsIdentifierStart(namedParameterQuery.originalQuery[i+1]) &&
			(i-1 >= 0 && namedParameterQuery.originalQuery[i-1] != ':') {

			j := i + 2
			for j < length && IsIdentifierPart(namedParameterQuery.originalQuery[j]) {
				j++
			}
			name := namedParameterQuery.originalQuery[i+1 : j]

			temp := namedParameterQuery.queryArguments.GetValue(":" + name)

			namedParameterQuery.parameters = append(namedParameterQuery.parameters, temp)
			c = '?'
			i += len(name)
		}
		if c == '?' {
			parsedQuery.WriteString("$" + fmt.Sprintf("%v", index))
			index++
		} else {
			parsedQuery.WriteString(string(c))
		}
	}

	namedParameterQuery.parsedQuery = parsedQuery.String()
}

func (namedParameterQuery *NamedParameterQuery) GetParsedQuery() string {
	return namedParameterQuery.parsedQuery
}

func (namedParameterQuery *NamedParameterQuery) GetParsedParameters() []interface{} {
	return namedParameterQuery.parameters
}

/*func (this *NamedParameterQuery) SetValuesFromStruct(parameters interface{}) error {

	var fieldValues reflect.Value
	var fieldValue reflect.Value
	var parameterType reflect.Type
	var parameterField reflect.StructField
	var queryTag string
	var visibilityCharacter rune

	fieldValues = reflect.ValueOf(parameters)

	if fieldValues.Kind() != reflect.Struct {
		return errors.New("Unable to add query values from parameter: parameter is not a struct")
	}

	parameterType = fieldValues.Type()

	for i := 0; i < fieldValues.NumField(); i++ {

		fieldValue = fieldValues.Field(i)
		parameterField = parameterType.Field(i)

		// public field?
		visibilityCharacter, _ = utf8.DecodeRuneInString(parameterField.Name[0:])

		if fieldValue.CanSet() || unicode.IsUpper(visibilityCharacter) {

			// check to see if this has a tag indicating a different query name
			queryTag = parameterField.Tag.Get("sqlParameterName")

			// otherwise just add the struct's name.
			if len(queryTag) <= 0 {
				queryTag = parameterField.Name
			}

			this.SetValue(queryTag, fieldValue.Interface())
		}
	}
	return nil
}*/
