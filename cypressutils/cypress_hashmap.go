package cypressutils

import (
	"bytes"
	"fmt"
	"github.com/codecypress/go-ancillary-utils/miscellaneous"
	"strconv"
	"strings"
)

type CypressHashMap struct {
	orderedMap *OrderedMap
	tableName  string
}

func NewMap(size ...int) *CypressHashMap {
	if size != nil {
		return &CypressHashMap{
			orderedMap: newOrderedMap(size[0]),
			tableName:  "[Undeclared]",
		}
	}
	return &CypressHashMap{
		orderedMap: newOrderedMap(),
		tableName:  "[Undeclared]",
	}
}

func (hashmap *CypressHashMap) GetTableName() string {
	return hashmap.tableName
}

func (hashmap *CypressHashMap) GetData() *OrderedMap {
	return hashmap.orderedMap
}

func (hashmap *CypressHashMap) SetTableName(tableName string) *CypressHashMap {
	hashmap.tableName = tableName
	return hashmap
}

func (hashmap *CypressHashMap) AddQueryArgument(column string, value interface{}) *CypressHashMap {
	return hashmap.PutValue(column, value)
}

func (hashmap *CypressHashMap) PutValue(column string, value interface{}) *CypressHashMap {
	hashmap.orderedMap.set(column, value)
	return hashmap
}

func (hashmap *CypressHashMap) GetValue(column string) interface{} {
	result, _ := hashmap.orderedMap.get(column)
	return result
}

func (hashmap *CypressHashMap) Contains(column string) bool {
	_, exists := hashmap.orderedMap.get(column)
	return exists
}

func (hashmap *CypressHashMap) GetStringValue(column string) string {
	result, exists := hashmap.orderedMap.get(column)

	if result == nil || !exists {
		return ""
	} else {
		return fmt.Sprintf("%v", result)
	}
}

func (hashmap *CypressHashMap) GetStringValueOrIfNull(column string, ifNullReplacement string) string {
	result, exists := hashmap.orderedMap.get(column)

	if result == nil || !exists {
		return ifNullReplacement
	} else {
		return fmt.Sprintf("%v", result)
	}
}

func (hashmap *CypressHashMap) RemoveColumn(column string) {
	hashmap.orderedMap.delete(column)
}

func (hashmap *CypressHashMap) GetAllColumns() (allColumns []string) {
	allColumns = make([]string, 0)

	for pair := hashmap.orderedMap.Oldest(); pair != nil; pair = pair.Next() {
		allColumns = append(allColumns, fmt.Sprintf("%v", pair.Key))
	}
	return
}

func (hashmap *CypressHashMap) GetKeysNoStartColon() (allColumns []string) {
	allColumns = make([]string, 0)

	for pair := hashmap.orderedMap.Oldest(); pair != nil; pair = pair.Next() {
		allColumns = append(allColumns, strings.ReplaceAll(fmt.Sprintf("%v", pair.Key), ":", ""))
	}
	return
}

func (hashmap *CypressHashMap) GetKeysWithStartColon() (allColumns []string) {
	allColumns = make([]string, 0)

	for pair := hashmap.orderedMap.Oldest(); pair != nil; pair = pair.Next() {
		strField := fmt.Sprintf("%v", pair.Key)
		if !strings.HasPrefix(strField, ":") && !strings.HasPrefix(strField, "@") {
			strField = ":" + strField
		}
		allColumns = append(allColumns, strField)
	}
	return
}

func (hashmap *CypressHashMap) GetAllColumnsDelimited(strDelimiter ...string) string {
	var delimiter = ","

	if strDelimiter != nil {
		delimiter = strDelimiter[0]
	}

	var buf bytes.Buffer
	var shouldAddComma = false
	for pair := hashmap.orderedMap.Oldest(); pair != nil; pair = pair.Next() {
		if shouldAddComma {
			buf.WriteString(delimiter)
		} else {
			shouldAddComma = true
		}

		buf.WriteString(fmt.Sprintf("%v", pair.Key))
	}

	return buf.String()
}

func (hashmap *CypressHashMap) Size() int {
	return hashmap.orderedMap.len()
}

func (hashmap *CypressHashMap) IsEmpty() bool {
	if hashmap == nil {
		return false
	}
	return hashmap.orderedMap.len() == 0
}

func (hashmap *CypressHashMap) CloneMe() *CypressHashMap {
	newMap := NewMap()

	for pair := hashmap.orderedMap.Oldest(); pair != nil; pair = pair.Next() {
		newMap.PutValue(fmt.Sprintf("%v", pair.Key), pair.Value)
	}
	return newMap
}

func (hashmap *CypressHashMap) CopyFrom(otherMap *CypressHashMap) *CypressHashMap {
	for pair := otherMap.orderedMap.Oldest(); pair != nil; pair = pair.Next() {
		hashmap.PutValue(fmt.Sprintf("%v", pair.Key), pair.Value)
	}
	return hashmap
}

func (hashmap *CypressHashMap) GetLongestField() int {
	maxLen := 0

	for pair := hashmap.orderedMap.Oldest(); pair != nil; pair = pair.Next() {
		field := fmt.Sprintf("%v", pair.Key)
		if fieldLen := len(field); fieldLen > maxLen {
			maxLen = fieldLen
		}
	}

	return maxLen
}

func (hashmap *CypressHashMap) PrintRecordTabular(exclude ...string) {
	excludedMap := map[string]interface{}{}
	if exclude != nil {
		for _, str := range exclude {
			excludedMap[str] = nil
		}
	}

	var excludedCount = 0
	if !hashmap.IsEmpty() {
		fmt.Println("\nTABLE: " + hashmap.tableName)
		fmt.Println("********************************")

		for pair := hashmap.orderedMap.Oldest(); pair != nil; pair = pair.Next() {
			field := fmt.Sprintf("%v", pair.Key)
			if _, shouldExclude := excludedMap[field]; shouldExclude {
				excludedCount++
				continue
			}

			field = strings.ToUpper(field)
			fmt.Printf("%-30s", miscellaneous.AbbreviateString(field, 30))
		}
		fmt.Println()
		for pair := hashmap.orderedMap.Oldest(); pair != nil; pair = pair.Next() {
			field := fmt.Sprintf("%v", pair.Key)
			if _, shouldExclude := excludedMap[field]; shouldExclude {
				continue
			}

			value := ""
			if pair.Value != nil {
				value = fmt.Sprintf("%v", pair.Value)
			} else {
				for i := 0; i < 30; i++ {
					value += " "
				}
			}

			fmt.Printf("%-30s", miscellaneous.AbbreviateString(value, 30))
		}
		fmt.Println()
		fmt.Println("\n*******************************")
		fmt.Println("TOTAL FIELDS =", hashmap.Size()-excludedCount)
	} else {
		fmt.Println("No record present")
	}
}

func (hashmap *CypressHashMap) PrintRecordVerticalLabelled(exclude ...string) {
	excludedMap := map[string]interface{}{}
	if exclude != nil {
		for _, str := range exclude {
			excludedMap[str] = nil
		}
	}

	var excludedCount = 0
	if !hashmap.IsEmpty() {
		fmt.Println("\nTABLE: " + hashmap.tableName)
		fmt.Println("********************************")

		longestField := hashmap.GetLongestField()
		for pair := hashmap.orderedMap.Oldest(); pair != nil; pair = pair.Next() {
			field := fmt.Sprintf("%v", pair.Key)

			if _, shouldExclude := excludedMap[field]; shouldExclude {
				excludedCount++
				continue
			}

			value := ""
			if pair.Value != nil {
				value = fmt.Sprintf("%v", pair.Value)
			}

			fmt.Printf("%"+strconv.Itoa(longestField)+"s: %s", field, value)

			fmt.Println()
		}
		fmt.Println("\n********************************")
		fmt.Println("TOTAL FIELDS =", hashmap.Size()-excludedCount)
	} else {
		fmt.Println("No record present")
	}
}

/*// this implements type json.Marshaler interface, so can be called in json.Marshal(om)
func (hashmap *CypressHashMap) MarshalJSON() (res []byte, err error) {
	res = append(res, '{')
	front, back := hashmap.orderedMap.list.Front(), hashmap.orderedMap.list.Back()
	for e := front; e != nil; e = e.Next() {
		k := e.Value.(string)
		res = append(res, fmt.Sprintf("%q:", k)...)
		var b []byte
		b, err = json.Marshal(hashmap.orderedMap.pairs[k])
		if err != nil {
			return
		}
		res = append(res, b...)
		if e != back {
			res = append(res, ',')
		}
	}
	res = append(res, '}')
	// fmt.Printf("marshalled: %v: %#v\n", res, res)
	return
}

// this implements type json.Unmarshaler interface, so can be called in json.Unmarshal(data, om)
func (om *OrderedMap) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	// must open with a delim token '{'
	t, err := dec.Token()
	if err != nil {
		return err
	}
	if delim, ok := t.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("expect JSON object open with '{'")
	}

	err = om.parseobject(dec)
	if err != nil {
		return err
	}

	t, err = dec.Token()
	if err != io.EOF {
		return fmt.Errorf("expect end of JSON object but got more token: %T: %v or err: %v", t, t, err)
	}

	return nil
}

func (om *OrderedMap) parseobject(dec *json.Decoder) (err error) {
	var t json.Token
	for dec.More() {
		t, err = dec.Token()
		if err != nil {
			return err
		}

		key, ok := t.(string)
		if !ok {
			return fmt.Errorf("expecting JSON key should be always a string: %T: %v", t, t)
		}

		t, err = dec.Token()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		var value interface{}
		value, err = handledelim(t, dec)
		if err != nil {
			return err
		}

		// om.keys = append(om.keys, key)
		om.keys[key] = om.l.PushBack(key)
		om.m[key] = value
	}

	t, err = dec.Token()
	if err != nil {
		return err
	}
	if delim, ok := t.(json.Delim); !ok || delim != '}' {
		return fmt.Errorf("expect JSON object close with '}'")
	}

	return nil
}

func parsearray(dec *json.Decoder) (arr []interface{}, err error) {
	var t json.Token
	arr = make([]interface{}, 0)
	for dec.More() {
		t, err = dec.Token()
		if err != nil {
			return
		}

		var value interface{}
		value, err = handledelim(t, dec)
		if err != nil {
			return
		}
		arr = append(arr, value)
	}
	t, err = dec.Token()
	if err != nil {
		return
	}

	if delim, ok := t.(json.Delim); !ok || delim != ']' {
		err = fmt.Errorf("expect JSON array close with ']'")
		return
	}

	return
}

func handledelim(t json.Token, dec *json.Decoder) (res interface{}, err error) {
	if delim, ok := t.(json.Delim); ok {
		switch delim {
		case '{':
			om2 := NewOrderedMap()
			err = om2.parseobject(dec)
			if err != nil {
				return
			}
			return om2, nil
		case '[':
			var value []interface{}
			value, err = parsearray(dec)
			if err != nil {
				return
			}
			return value, nil
		default:
			return nil, fmt.Errorf("Unexpected delimiter: %q", delim)
		}
	}
	return t, nil
}*/
