package cypressutils

import (
	"bytes"
	"runtime/debug"
	"strings"
)

const const_WHITESPACE = " \n\r\f\t"
const const_INDENT_STRING = "    "
const const_INITIAL = "\n"

var var_BEGIN_CLAUSES = NewSet()
var var_END_CLAUSES = NewSet()
var var_LOGICAL = NewSet()
var var_QUANTIFIERS = NewSet()
var var_DML = NewSet()
var var_MISC = NewSet()

func init() {
	var_BEGIN_CLAUSES.Add("left")
	var_BEGIN_CLAUSES.Add("right")
	var_BEGIN_CLAUSES.Add("inner")
	var_BEGIN_CLAUSES.Add("outer")
	var_BEGIN_CLAUSES.Add("group")
	var_BEGIN_CLAUSES.Add("order")

	var_END_CLAUSES.Add("where")
	var_END_CLAUSES.Add("set")
	var_END_CLAUSES.Add("having")
	var_END_CLAUSES.Add("join")
	var_END_CLAUSES.Add("from")
	var_END_CLAUSES.Add("by")
	var_END_CLAUSES.Add("join")
	var_END_CLAUSES.Add("into")
	var_END_CLAUSES.Add("union")

	var_LOGICAL.Add("and")
	var_LOGICAL.Add("or")
	var_LOGICAL.Add("when")
	var_LOGICAL.Add("else")
	var_LOGICAL.Add("end")

	var_QUANTIFIERS.Add("in")
	var_QUANTIFIERS.Add("all")
	var_QUANTIFIERS.Add("exists")
	var_QUANTIFIERS.Add("some")
	var_QUANTIFIERS.Add("any")

	var_DML.Add("insert")
	var_DML.Add("update")
	var_DML.Add("delete")

	var_MISC.Add("select")
	var_MISC.Add("on")
}

type formatProcess struct {
	beginLine                  bool
	afterBeginBeforeEnd        bool
	afterByOrSetOrFromOrSelect bool
	afterValues                bool
	afterOn                    bool
	afterBetween               bool
	afterInsert                bool
	inFunction                 int
	parensSinceSelect          int

	parenCounts            []int
	afterByOrFromOrSelects []bool

	indent int

	result bytes.Buffer
	tokens *StringTokenizer

	lastToken, token, lcToken string
}

func newFormatProcess(sql string) *formatProcess {
	return &formatProcess{
		beginLine:              true,
		indent:                 1,
		tokens:                 NewStringTokenizer(sql, "()+*/-=<>'`\"[],"+const_WHITESPACE, true),
		parenCounts:            []int{},
		afterByOrFromOrSelects: []bool{},
	}
}

func (formatter *formatProcess) perform() string {
	formatter.result.WriteString(const_INITIAL)

	for formatter.tokens.HasMoreTokens() {
		formatter.token = formatter.tokens.NextToken()
		formatter.lcToken = strings.ToLower(formatter.token)

		if formatter.token == "'" {
			var t string

			for ok := true; ok; ok = t != "'" && formatter.tokens.HasMoreTokens() {
				t = formatter.tokens.NextToken()
				formatter.token += t
			}

		} else if formatter.token == "\"" {
			var t string
			for ok := true; ok; ok = t != "\"" {
				t = formatter.tokens.NextToken()
				formatter.token += t
			}
		}

		if formatter.afterByOrSetOrFromOrSelect && formatter.token == "," {
			formatter.commaAfterByOrFromOrSelect()
		} else if formatter.afterOn && formatter.token == "," {
			formatter.commaAfterOn()
		} else if formatter.token == "(" {
			formatter.openParen()
		} else if formatter.token == ")" {
			formatter.closeParen()
		} else if var_BEGIN_CLAUSES.Contains(formatter.lcToken) {
			formatter.beginNewClause()
		} else if var_END_CLAUSES.Contains(formatter.lcToken) {
			formatter.endNewClause()
		} else if formatter.token == "select" {
			formatter.selectFunc()
		} else if var_DML.Contains(formatter.lcToken) {
			formatter.updateOrInsertOrDelete()
		} else if formatter.token == "values" {
			formatter.values()
		} else if formatter.token == "on" {
			formatter.on()
		} else if formatter.afterBetween && formatter.lcToken == "and" {
			formatter.misc()
			formatter.afterBetween = false
		} else if var_LOGICAL.Contains(formatter.lcToken) {
			formatter.logical()
		} else if isWhitespace(formatter.token) {
			formatter.white()
		} else {
			formatter.misc()
		}

		if !isWhitespace(formatter.token) {
			formatter.lastToken = formatter.lcToken
		}
	}

	return formatter.result.String()
}

func (formatter *formatProcess) commaAfterOn() {
	formatter.out()
	formatter.indent--
	formatter.newline()
	formatter.afterOn = false
	formatter.afterByOrSetOrFromOrSelect = true
}

func (formatter *formatProcess) commaAfterByOrFromOrSelect() {
	formatter.out()
	formatter.newline()
}

func (formatter *formatProcess) logical() {
	if "end" == formatter.lcToken {
		formatter.indent--
	}
	formatter.newline()
	formatter.out()
	formatter.beginLine = false
}

func (formatter *formatProcess) on() {
	formatter.indent++
	formatter.afterOn = true
	formatter.newline()
	formatter.out()
	formatter.beginLine = false
}

func (formatter *formatProcess) misc() {
	formatter.out()
	if "between" == formatter.lcToken {
		formatter.afterBetween = true
	}
	if formatter.afterInsert {
		formatter.newline()
		formatter.afterInsert = false
	} else {
		formatter.beginLine = false
		if "case" == formatter.lcToken {
			formatter.indent++
		}
	}
}

func (formatter *formatProcess) white() {
	if !formatter.beginLine {
		formatter.result.WriteString(" ")
	}
}

func (formatter *formatProcess) updateOrInsertOrDelete() {
	formatter.out()
	formatter.indent++
	formatter.beginLine = false
	if "update" == formatter.lcToken {
		formatter.newline()
	}
	if "insert" == formatter.lcToken {
		formatter.afterInsert = true
	}
}

func (formatter *formatProcess) selectFunc() {
	formatter.out()
	formatter.indent++
	formatter.newline()
	formatter.parenCounts = append(formatter.parenCounts, formatter.parensSinceSelect)
	formatter.afterByOrFromOrSelects = append(formatter.afterByOrFromOrSelects, formatter.afterByOrSetOrFromOrSelect)
	formatter.parensSinceSelect = 0
	formatter.afterByOrSetOrFromOrSelect = true
}

func (formatter *formatProcess) out() {
	formatter.result.WriteString(formatter.token)
}

func (formatter *formatProcess) endNewClause() {
	if !formatter.afterBeginBeforeEnd {
		formatter.indent--
		if formatter.afterOn {
			formatter.indent--
			formatter.afterOn = false
		}
		formatter.newline()
	}
	formatter.out()
	if "union" != formatter.lcToken {
		formatter.indent++
	}
	formatter.newline()
	formatter.afterBeginBeforeEnd = false
	formatter.afterByOrSetOrFromOrSelect = "by" == formatter.lcToken ||
		"set" == formatter.lcToken || "from" == formatter.lcToken
}

func (formatter *formatProcess) beginNewClause() {
	if !formatter.afterBeginBeforeEnd {
		if formatter.afterOn {
			formatter.indent--
			formatter.afterOn = false
		}
		formatter.indent--
		formatter.newline()
	}
	formatter.out()
	formatter.beginLine = false
	formatter.afterBeginBeforeEnd = true
}

func (formatter *formatProcess) values() {
	formatter.indent--
	formatter.newline()
	formatter.out()
	formatter.indent++
	formatter.newline()
	formatter.afterValues = true
}

func (formatter *formatProcess) closeParen() {
	formatter.parensSinceSelect--
	if formatter.parensSinceSelect < 0 {
		formatter.indent--

		temp1 := formatter.parenCounts[len(formatter.parenCounts)-1]
		formatter.parenCounts = formatter.parenCounts[:len(formatter.parenCounts)-1]

		formatter.parensSinceSelect = temp1

		temp2 := formatter.afterByOrFromOrSelects[len(formatter.afterByOrFromOrSelects)-1]
		formatter.afterByOrFromOrSelects = formatter.afterByOrFromOrSelects[:len(formatter.afterByOrFromOrSelects)-1]

		formatter.afterByOrSetOrFromOrSelect = temp2
	}
	if formatter.inFunction > 0 {
		formatter.inFunction--
		formatter.out()
	} else {
		if !formatter.afterByOrSetOrFromOrSelect {
			formatter.indent--
			formatter.newline()
		}
		formatter.out()
	}
	formatter.beginLine = false
}

func (formatter *formatProcess) openParen() {
	if isFunctionName(formatter.lastToken) || formatter.inFunction > 0 {
		formatter.inFunction++
	}
	formatter.beginLine = false
	if formatter.inFunction > 0 {
		formatter.out()
	} else {
		formatter.out()
		if !formatter.afterByOrSetOrFromOrSelect {
			formatter.indent++
			formatter.newline()
			formatter.beginLine = true
		}
	}
	formatter.parensSinceSelect++
}

func isFunctionName(tok string) bool {
	return !var_LOGICAL.Contains(tok) &&
		!var_END_CLAUSES.Contains(tok) &&
		!var_QUANTIFIERS.Contains(tok) &&
		!var_DML.Contains(tok) &&
		!var_MISC.Contains(tok)
}

func isWhitespace(token string) bool {
	return strings.Contains(const_WHITESPACE, token)
}

func (formatter *formatProcess) newline() {
	formatter.result.WriteString("\n")
	for i := 0; i < formatter.indent; i++ {
		formatter.result.WriteString(const_INDENT_STRING)
	}
	formatter.beginLine = true
}

func FormatSQL(source string) string {
	defer func() {
		if r := recover(); r != nil {
			debug.PrintStack()
		}
	}()

	formatter := newFormatProcess(source)
	theQuery := formatter.perform()
	return theQuery
}
