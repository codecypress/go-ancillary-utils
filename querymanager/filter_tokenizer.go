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

const const_COMBINERS_DELIMITER = "|"
const const_SINGLE_CLAUSE_DELIMITER = ":"

var var_COMBINERS = cypressutils.NewSet()

var var_RELATIONS_AND_SYMBOLS = make(map[string]string)
var var_RELATIONS_AND_FULL_NAMES = make(map[string]string)

func init() {
	var_COMBINERS.Add("AND")
	var_COMBINERS.Add("OR")

	var_RELATIONS_AND_SYMBOLS["eq"] = "="
	var_RELATIONS_AND_FULL_NAMES["eq"] = "Equal To"
	var_RELATIONS_AND_SYMBOLS["!eq"] = "<>"
	var_RELATIONS_AND_FULL_NAMES["!eq"] = "Not Equal To"

	var_RELATIONS_AND_SYMBOLS["gt"] = ">"
	var_RELATIONS_AND_FULL_NAMES["gt"] = "Greater Than"
	var_RELATIONS_AND_SYMBOLS["gte"] = ">="
	var_RELATIONS_AND_FULL_NAMES["gte"] = "Greater Than Or Equal To"
	var_RELATIONS_AND_SYMBOLS["lt"] = "<"
	var_RELATIONS_AND_FULL_NAMES["lt"] = "Less Than"
	var_RELATIONS_AND_SYMBOLS["lte"] = "<="
	var_RELATIONS_AND_FULL_NAMES["lte"] = "Less Than Or Equal To"

	var_RELATIONS_AND_SYMBOLS["contains"] = "LIKE"
	var_RELATIONS_AND_FULL_NAMES["contains"] = "Contains"
	var_RELATIONS_AND_SYMBOLS["btwn"] = "BETWEEN"
	var_RELATIONS_AND_FULL_NAMES["btwn"] = "Between"
	var_RELATIONS_AND_SYMBOLS["in"] = "IN"
	var_RELATIONS_AND_FULL_NAMES["in"] = "In"
	var_RELATIONS_AND_SYMBOLS["null"] = "IS NULL"
	var_RELATIONS_AND_FULL_NAMES["null"] = "Is Null"
	var_RELATIONS_AND_SYMBOLS["sw"] = "LIKE"
	var_RELATIONS_AND_FULL_NAMES["sw"] = "Starts With"
	var_RELATIONS_AND_SYMBOLS["ew"] = "LIKE"
	var_RELATIONS_AND_FULL_NAMES["ew"] = "Ends With"

	var_RELATIONS_AND_SYMBOLS["!sw"] = "NOT LIKE"
	var_RELATIONS_AND_FULL_NAMES["!sw"] = "Not Starting With"
	var_RELATIONS_AND_SYMBOLS["!ew"] = "NOT LIKE"
	var_RELATIONS_AND_FULL_NAMES["!ew"] = "Not Ending With"
	var_RELATIONS_AND_SYMBOLS["!contains"] = "NOT LIKE"
	var_RELATIONS_AND_FULL_NAMES["!contains"] = "Not containing"
	var_RELATIONS_AND_SYMBOLS["!btwn"] = "NOT BETWEEN"
	var_RELATIONS_AND_FULL_NAMES["!btwn"] = "Not Between"
	var_RELATIONS_AND_SYMBOLS["!in"] = "NOT IN"
	var_RELATIONS_AND_FULL_NAMES["!in"] = "Not In"
	var_RELATIONS_AND_SYMBOLS["!null"] = "IS NOT NULL"
	var_RELATIONS_AND_FULL_NAMES["!null"] = "Is Not Null"
}

func GenerateFilterString(filterStatement string) (string, *cypressutils.CypressHashMap, *cypressutils.Set, error) {
	err := validateCurlyBraces(filterStatement)
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return "", nil, nil, err
	}

	err = validateParentheses(filterStatement)
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
		return "", nil, nil, err
	}

	filterStatement = stripCurlyBraces(filterStatement)

	fPredicate, err := newFormPredicate(filterStatement)

	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
	}

	filter, arguments, columns := fPredicate.getFilterPredicate()
	return filter, arguments, columns, err
}

func validateCurlyBraces(filterStatement string) error {
	if strings.Count(filterStatement, "{") > 1 || strings.Count(filterStatement, "}") > 1 {
		return cErrors.New("Only one of '{}' are allowed in the filter. Offender: " + filterStatement)
	}

	if !strings.HasPrefix(filterStatement, "{") {
		return cErrors.New("Filter must start with '{'. Offender: " + filterStatement)
	}

	if !strings.HasSuffix(filterStatement, "}") {
		return cErrors.New("Filter must end with '}'. Offender: " + filterStatement)
	}

	return nil
}

func validateParentheses(filterStatement string) error {
	if strings.Count(filterStatement, "(") != strings.Count(filterStatement, ")") {
		return cErrors.New("The number of '(' must match number of ')'. Offender: " + filterStatement)
	}
	return nil
}

func stripCurlyBraces(filterStatement string) string {
	str := strings.ReplaceAll(filterStatement, "{", "")
	str = strings.ReplaceAll(str, "}", "")
	return str
}

type formPredicate struct {
	filterStatement           string
	filterPredicate           bytes.Buffer
	filterColumns             *cypressutils.Set
	queryArgsCounter          int
	queryArguments            *cypressutils.CypressHashMap
	nextIsCombinerOperator    bool
	shouldRecurse             bool
	checkIllegalCombinerStart bool
	or                        string
	and                       string
}

func newFormPredicate(filterStatement string) (*formPredicate, error) {
	fPredicate := &formPredicate{
		filterStatement:           filterStatement,
		filterColumns:             cypressutils.NewSet(),
		queryArgsCounter:          0,
		queryArguments:            cypressutils.NewMap(),
		nextIsCombinerOperator:    false,
		shouldRecurse:             false,
		checkIllegalCombinerStart: true,
		or:                        const_COMBINERS_DELIMITER + "\\s*OR\\s*" + const_COMBINERS_DELIMITER,
		and:                       const_COMBINERS_DELIMITER + "\\s*AND\\s*" + const_COMBINERS_DELIMITER,
	}

	err := fPredicate.perform()
	if err != nil {
		exceptions.ThrowException(cErrors.Cause(err))
	}

	return fPredicate, err
}

func (fPredicate *formPredicate) getFilterPredicate() (string, *cypressutils.CypressHashMap, *cypressutils.Set) {
	return "(" + fPredicate.filterPredicate.String() + ")", fPredicate.queryArguments, fPredicate.filterColumns
}

func (fPredicate *formPredicate) perform() error {
	fPredicate.filterStatement = strings.TrimSpace(fPredicate.filterStatement)
	if fPredicate.filterStatement == "" {
		return cErrors.New("Empty filter statement provided")
	}

	if strings.HasPrefix(fPredicate.filterStatement, "(") {
		fPredicate.filterPredicate.WriteString("(")
		reStr := regexp.MustCompile("^(.*?)\\((.*)$")
		repStr := "${1}$2"
		fPredicate.filterStatement = reStr.ReplaceAllString(fPredicate.filterStatement, repStr)

		reg := "^(" + fPredicate.or + "?|" + fPredicate.and + ").*$"

		if matches, _ := regexp.MatchString(reg, fPredicate.filterStatement); matches {
			return cErrors.New("Combiner operator after '(' without a preceding clause " + "disallowed. Offender: " + fPredicate.filterStatement)
		}

		err := fPredicate.perform()
		if err != nil {
			return err
		}
	}

	if fPredicate.checkIllegalCombinerStart {
		fPredicate.checkIllegalCombinerStart = false
		reg := "^(" + fPredicate.or + "?|" + fPredicate.and + ").*$"

		if matches, _ := regexp.MatchString(reg, fPredicate.filterStatement); matches {
			return cErrors.New("First combiner operator without a preceding clause " + "disallowed. Offender: " + fPredicate.filterStatement)
		}
	}

	iOp := strings.Index(fPredicate.filterStatement, "(")
	iCp := strings.Index(fPredicate.filterStatement, ")")

	if iOp >= 0 {
		if iOp < iCp {
			err := fPredicate.evaluateCombinedClause(fPredicate.filterStatement[0:iOp])
			if err != nil {
				return err
			}
		} else {
			err := fPredicate.evaluateCombinedClause(fPredicate.filterStatement[0:iCp])
			if err != nil {
				return err
			}
			fPredicate.nextIsCombinerOperator = true
		}
		fPredicate.shouldRecurse = true
	} else if iCp >= 0 {
		err := fPredicate.evaluateCombinedClause(fPredicate.filterStatement[0:iCp])
		if err != nil {
			return err
		}

		fPredicate.nextIsCombinerOperator = true
		fPredicate.shouldRecurse = true
	} else {
		if fPredicate.filterStatement != "" {
			err := fPredicate.evaluateCombinedClause(fPredicate.filterStatement)
			if err != nil {
				return err
			}
		}
		fPredicate.shouldRecurse = false
	}
	if fPredicate.shouldRecurse {
		err := fPredicate.perform()
		if err != nil {
			return err
		}
	}
	return nil
}

func (fPredicate *formPredicate) evaluateCombinedClause(semiClause string) error {
	combinedClauses := strings.Split(semiClause, const_COMBINERS_DELIMITER)
	for i := range combinedClauses {
		singleClause := strings.TrimSpace(combinedClauses[i])

		reStr := regexp.MustCompile("^(.*?)" + const_COMBINERS_DELIMITER + "\\s*(" + singleClause + ")\\s*" + const_COMBINERS_DELIMITER + "(.*)$")
		repStr := "${1}$2"
		fPredicate.filterStatement = reStr.ReplaceAllString(fPredicate.filterStatement, repStr)
		fPredicate.filterStatement = strings.TrimSpace(fPredicate.filterStatement)
		if singleClause != "" {
			if strings.HasPrefix(fPredicate.filterStatement, "(") || strings.HasPrefix(fPredicate.filterStatement, ")") {
				fPredicate.nextIsCombinerOperator = true
			}

			if fPredicate.nextIsCombinerOperator {
				if !var_COMBINERS.Contains(singleClause) {
					return cErrors.New("Combiner Operator " + singleClause + " disallowed. Offender: " + semiClause)
				}

				if i == len(combinedClauses)-1 &&
					!strings.HasPrefix(fPredicate.filterStatement, "(") &&
					!strings.HasPrefix(fPredicate.filterStatement, ")") {

					return cErrors.New("Ending combiner operator " + singleClause + " without a succeeding clause " +
						"disallowed. Offender: " + semiClause)
				}

				fPredicate.filterPredicate.WriteString(singleClause)
				fPredicate.filterPredicate.WriteString(" ")
			} else {
				err := fPredicate.evaluateSingleClause(singleClause)
				if err != nil {
					return err
				}
			}

			fPredicate.nextIsCombinerOperator = !fPredicate.nextIsCombinerOperator
		} else {
			fPredicate.filterPredicate.WriteString(" ")
		}
	}

	fPredicate.filterStatement = strings.TrimSpace(fPredicate.filterStatement)

	if strings.HasPrefix(fPredicate.filterStatement, ")") {
		tempString := fPredicate.filterPredicate.String()
		fPredicate.filterPredicate.Reset()
		fPredicate.filterPredicate.WriteString(strings.TrimSpace(tempString))
		fPredicate.filterPredicate.WriteString(")")

		reStr := regexp.MustCompile("^(.*?)\\)(.*)$")
		repStr := "${1}$2"
		fPredicate.filterStatement = reStr.ReplaceAllString(fPredicate.filterStatement, repStr)
		fPredicate.filterStatement = strings.TrimSpace(fPredicate.filterStatement)

		fPredicate.nextIsCombinerOperator = true
	}

	return nil
}

func (fPredicate *formPredicate) evaluateSingleClause(singleClause string) error {
	components := split(singleClause, const_SINGLE_CLAUSE_DELIMITER)

	if len(components) < 2 {
		return cErrors.New("Clause missing the corresponding operation. Offender: " + singleClause)
	}

	if _, exists := var_RELATIONS_AND_SYMBOLS[strings.TrimSpace(components[1])]; !exists {
		return cErrors.New("Unresolvable Operator '" + components[1] + "'. Offender: " + singleClause)
	}

	columnName := strings.TrimSpace(components[0])
	relationName := strings.TrimSpace(components[1])

	fPredicate.filterPredicate.WriteString(columnName)
	fPredicate.filterPredicate.WriteString(" ")
	fPredicate.filterColumns.Add(columnName)

	clnNameNoAlias := columnName

	if matches, _ := regexp.MatchString("[a-zA-Z]+\\.+[a-zA-Z0-9_]+", columnName); matches {
		reStr := regexp.MustCompile("^(.*?)\\.(.*)$")
		repStr := "${1}__$2"

		clnNameNoAlias = reStr.ReplaceAllString(columnName, repStr)
	}

	switch relationName {
	case "btwn", "!btwn":
		{
			if len(components) < 3 {
				return cErrors.New("Between expects two values that are comma separated. Offender: " + singleClause)
			}

			btnOperands := strings.Split(strings.TrimSpace(components[2]), ",")
			if len(btnOperands) < 2 {
				return cErrors.New("Between expects two values that are comma separated. Offender: " + singleClause)
			}

			fPredicate.filterPredicate.WriteString(var_RELATIONS_AND_SYMBOLS[relationName])
			fPredicate.filterPredicate.WriteString(" ")
			fPredicate.filterPredicate.WriteString(":" + fmt.Sprintf("%s%d", clnNameNoAlias, fPredicate.queryArgsCounter))
			fPredicate.filterPredicate.WriteString(" AND ")
			fPredicate.filterPredicate.WriteString(":" + fmt.Sprintf("%s%d", clnNameNoAlias, fPredicate.queryArgsCounter+1))
			fPredicate.filterPredicate.WriteString(" ")

			reStr := regexp.MustCompile("[^A-Za-z0-9_]")
			temp := reStr.ReplaceAllString(clnNameNoAlias, "_")

			fPredicate.queryArguments.AddQueryArgument(":"+fmt.Sprintf("%s%d", temp, fPredicate.queryArgsCounter), strings.TrimSpace(btnOperands[0]))
			fPredicate.queryArguments.AddQueryArgument(":"+fmt.Sprintf("%s%d", temp, fPredicate.queryArgsCounter+1), strings.TrimSpace(btnOperands[1]))

			break
		}

	case "in", "!in":
		{
			if len(components) < 3 {
				return cErrors.New("IN expects at least one value. Offender: " + singleClause)
			}

			fPredicate.filterPredicate.WriteString(var_RELATIONS_AND_SYMBOLS[relationName])
			fPredicate.filterPredicate.WriteString(" (")

			inValues := strings.Split(components[2], ",")
			length := len(inValues)

			shouldAddComma := false
			for i := 0; i < length; i++ {
				if shouldAddComma {
					fPredicate.filterPredicate.WriteString(",")
				} else {
					shouldAddComma = true
				}

				fPredicate.filterPredicate.WriteString(":" + fmt.Sprintf("%s%d", clnNameNoAlias, fPredicate.queryArgsCounter+i))

				reStr := regexp.MustCompile("[^A-Za-z0-9_]")
				temp := reStr.ReplaceAllString(clnNameNoAlias, "_")

				fPredicate.queryArguments.AddQueryArgument(":"+fmt.Sprintf("%s%d", temp, fPredicate.queryArgsCounter+i), strings.TrimSpace(inValues[i]))
			}
			fPredicate.filterPredicate.WriteString(") ")
			break
		}

	case "null", "!null":
		{
			fPredicate.filterPredicate.WriteString(var_RELATIONS_AND_SYMBOLS[relationName])
			fPredicate.filterPredicate.WriteString(" ")
			break
		}
	default:

		if len(components) < 3 {
			return cErrors.New(var_RELATIONS_AND_SYMBOLS[relationName] + " expects at least one value. Offender: " + singleClause)
		}

		fPredicate.filterPredicate.WriteString(var_RELATIONS_AND_SYMBOLS[relationName])
		fPredicate.filterPredicate.WriteString(" ")
		fPredicate.filterPredicate.WriteString(":" + fmt.Sprintf("%s%d", clnNameNoAlias, fPredicate.queryArgsCounter))
		fPredicate.filterPredicate.WriteString(" ")

		reStr := regexp.MustCompile("[^A-Za-z0-9_]")
		temp := reStr.ReplaceAllString(clnNameNoAlias, "_")

		switch relationName {
		case "contains", "!contains":
			{
				fPredicate.queryArguments.AddQueryArgument(":"+fmt.Sprintf("%s%d", temp, fPredicate.queryArgsCounter), "%"+strings.TrimSpace(components[2])+"%")
				break
			}
		case "sw", "!sw":
			{
				fPredicate.queryArguments.AddQueryArgument(":"+fmt.Sprintf("%s%d", temp, fPredicate.queryArgsCounter), strings.TrimSpace(components[2])+"%")
				break
			}
		case "ew", "!ew":
			{
				fPredicate.queryArguments.AddQueryArgument(":"+fmt.Sprintf("%s%d", temp, fPredicate.queryArgsCounter), "%"+strings.TrimSpace(components[2]))
				break
			}
		default:
			{
				fPredicate.queryArguments.AddQueryArgument(":"+fmt.Sprintf("%s%d", temp, fPredicate.queryArgsCounter), strings.TrimSpace(components[2]))
				break
			}
		}
		break
	}

	reStr := regexp.MustCompile("^(.*?)" + singleClause + "(.*)$")
	repStr := "${1}$2"
	fPredicate.filterStatement = reStr.ReplaceAllString(fPredicate.filterStatement, repStr)
	fPredicate.filterStatement = strings.TrimSpace(fPredicate.filterStatement)
	fPredicate.queryArgsCounter++

	return nil
}

func split(str, delem string) (list []string) {
	charArr := []uint8(str)
	delemArr := []uint8(delem)

	counter := 0
	stopCount := 0

	charLen := len(charArr)
	delemLen := len(delemArr)

	for i := 0; i < charLen; i++ {
		k := 0
		for j := 0; j < delemLen; j++ {
			if charArr[i+j] == delemArr[j] {
				k++
			} else {
				break
			}
		}

		if k == delemLen {
			s := ""
			for counter < i {
				s += string(charArr[counter])
				counter++
			}
			i = i + k
			counter = i
			list = append(list, s)

			if stopCount == 1 {
				break
			}
			stopCount++
		}
	}

	s := ""
	if counter < charLen {
		for counter < charLen {
			s += string(charArr[counter])
			counter++
		}
		list = append(list, s)
	}
	return list
}
