package cypressutils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/codecypress/go-ancillary-utils/structsreflect"
	jsoniter "github.com/json-iterator/go"
	cErrors "github.com/pkg/errors"
	"io"
)

// this implements type json.Marshaler interface, so can be called in json.Marshal(om)

/*func ToJSON(hashmap *cypressutils.CypressHashMap) (resultStr string, err error) {
	var jsonIter = jsoniter.ConfigCompatibleWithStandardLibrary

	var result []byte
	//result.WriteString("{")
	result = append(result, '{')

	shouldAddComma := false
	for pair := hashmap.GetData().Oldest(); pair != nil; pair = pair.Next() {
		if shouldAddComma {
			result = append(result, ',')
			//result.WriteString(",")
		} else {
			shouldAddComma = true
		}

		k := pair.Key.(string)
		result = append(result, fmt.Sprintf("%q:", k)...)

		var b []byte

		b, err = jsonIter.Marshal(hashmap.GetValue(k))
		if err != nil {
			return
		}
		result = append(result, b...)
	}

	result = append(result, '}')

	return string(result), nil

	// fmt.Printf("marshalled: %v: %#v\n", res, res)
}
*/

func ToJSON(theData interface{}, jsonIter_ ...*jsoniter.API) (resultStr string, err error) {
	var jsonIter *jsoniter.API
	if jsonIter_ != nil {
		jsonIter = jsonIter_[0]
	} else {
		jsonIter = &jsoniter.ConfigCompatibleWithStandardLibrary
	}

	if object, exists := theData.(interface{ ToJsonString() (string, error) }); exists {
		return object.ToJsonString()
	}

	switch theData.(type) {
	case *CypressArrayList:
		{
			if theData == nil {
				return "[]", nil
			}
			return marshalCypressList(jsonIter, theData.(*CypressArrayList))
		}
	case *CypressHashMap:
		{
			if theData == nil {
				return "{}", nil
			}
			return marshalCypressHashMap(jsonIter, theData.(*CypressHashMap))
		}
	case *PageableWrapper, *TransactionWrapper:
		{
			return marshalComplexObject(jsonIter, theData)
		}
	default:
		{
			//var dst bytes.Buffer
			//enc := jsoniter.NewEncoder(&dst)
			//enc.SetEscapeHTML(false)
			//enc.SetIndent("","\t")
			//err := enc.Encode(theData)
			//if err != nil {
			//	return "", err
			//}
			//
			//return dst.String(), nil

			//return dst.Bytes()[:dst.Len() - 1], nil

			return (*jsonIter).MarshalToString(theData)
		}
	}
}

func marshalCypressList(jsonIter *jsoniter.API, theList *CypressArrayList) (resultStr string, err error) {
	var result []byte
	result = append(result, '[')

	length := theList.Size()
	shouldAddComma := false
	for i := 0; i < length; i++ {
		hashMap := theList.GetRecord(i)

		if shouldAddComma {
			result = append(result, ',')
			//result.WriteString(",")
		} else {
			shouldAddComma = true
		}

		tempResultStr, err := marshalCypressHashMap(jsonIter, hashMap)
		if err != nil {
			ThrowException(cErrors.Cause(err))
			return "", err
		}
		result = append(result, []byte(tempResultStr)...)
	}
	result = append(result, ']')
	return string(result), nil
}

func marshalCypressHashMap(jsonIter *jsoniter.API, theHashMap *CypressHashMap) (resultStr string, err error) {
	if theHashMap == nil || theHashMap.GetData() == nil {
		return "{}", nil
	}

	var result []byte
	//result.WriteString("{")
	result = append(result, '{')

	shouldAddComma := false
	for pair := theHashMap.GetData().Oldest(); pair != nil; pair = pair.Next() {
		if shouldAddComma {
			result = append(result, ',')
			//result.WriteString(",")
		} else {
			shouldAddComma = true
		}

		key := pair.Key.(string)
		theValue := pair.Value

		result = append(result, fmt.Sprintf("%q:", key)...)

		var tempResultStr string
		switch theValue.(type) {
		case *CypressHashMap:
			{
				tempResultStr, err = marshalCypressHashMap(jsonIter, theValue.(*CypressHashMap))
				if err != nil {
					ThrowException(cErrors.Cause(err))
					return "", err
				}
				result = append(result, tempResultStr...)
			}
		case *CypressArrayList:
			{
				tempResultStr, err = marshalCypressList(jsonIter, theValue.(*CypressArrayList))
				if err != nil {
					ThrowException(cErrors.Cause(err))
					return "", err
				}
				result = append(result, tempResultStr...)
			}
		case *PageableWrapper, *TransactionWrapper:
			{
				tempResultStr, err = marshalComplexObject(jsonIter, theValue)
				if err != nil {
					ThrowException(cErrors.Cause(err))
					return "", err
				}
				result = append(result, tempResultStr...)
			}
		default:
			{
				/*var dst bytes.Buffer
				enc := jsoniter.NewEncoder(&dst)
				enc.SetEscapeHTML(false)
				enc.SetIndent("","\t")
				err = enc.Encode(theValue)
				if err != nil {
					exceptions.ThrowException(cErrors.Cause(err))
					return "", err
				}
				result = append(result, dst.Bytes()...)*/

				var b []byte

				b, err = (*jsonIter).Marshal(theValue)
				if err != nil {
					ThrowException(cErrors.Cause(err))
					return
				}

				result = append(result, b...)
			}
		}
	}

	result = append(result, '}')

	return string(result), nil
}

/*func marshalPageableWrapper(jsonIter *jsoniter.API, thePageableWrapper *cypressutils.PageableWrapper) (resultStr string, err error) {
	var result []byte

	result = append(result, '{')

	result = append(result, fmt.Sprintf("%q:", "domain")...)
	result = append(result, fmt.Sprintf("%q:", thePageableWrapper.GetDomain())...)
	result = append(result, ',')

	result = append(result, fmt.Sprintf("%q:", "current_page")...)
	result = append(result, fmt.Sprintf("%v:", thePageableWrapper.GetCurrentPage())...)
	result = append(result, ',')

	result = append(result, fmt.Sprintf("%q:", "last_page")...)
	result = append(result, fmt.Sprintf("%v:", thePageableWrapper.GetLastPage())...)
	result = append(result, ',')

	result = append(result, fmt.Sprintf("%q:", "page_page")...)
	result = append(result, fmt.Sprintf("%v:", thePageableWrapper.GetPageSize())...)
	result = append(result, ',')

	result = append(result, fmt.Sprintf("%q:", "total_count")...)
	result = append(result, fmt.Sprintf("%v:", thePageableWrapper.GetTotalCount())...)
	result = append(result, ',')

	result = append(result, fmt.Sprintf("%q:", "total_count")...)
	tempStr, err := ToJSON(thePageableWrapper.GetData(), jsonIter)

	if err != nil {
		return "", err
	}

	result = append(result, tempStr...)
	result = append(result, '}')
	return string(result), nil
}*/

func marshalComplexObject(jsonIter *jsoniter.API, s interface{}) (resultStr string, err error) {
	var result []byte

	fields := structsreflect.Fields(s)

	result = append(result, '{')

	shouldAddComma := false
	for index := range fields {
		if shouldAddComma {
			result = append(result, ',')
		} else {
			shouldAddComma = true
		}

		field := fields[index]

		key := field.Name()

		if field.Tag("json") != "" {
			key = field.Tag("json")
		}

		result = append(result, fmt.Sprintf("%q:", key)...)

		switch field.Value().(type) {
		case *CypressHashMap, *CypressArrayList, *PageableWrapper, *TransactionWrapper:
			{
				tempStr, err := ToJSON(field.Value(), jsonIter)
				if err != nil {
					return "", err
				}

				result = append(result, tempStr...)
			}
		default:
			{
				/*var dst bytes.Buffer
				enc := jsoniter.NewEncoder(&dst)
				enc.SetEscapeHTML(false)
				enc.SetIndent("","\t")
				err = enc.Encode(field.Value())
				if err != nil {
					exceptions.ThrowException(cErrors.Cause(err))
					return "", err
				}
				result = append(result, dst.Bytes()...)*/

				var b []byte

				b, err = (*jsonIter).Marshal(field.Value())
				if err != nil {
					ThrowException(cErrors.Cause(err))
					return
				}
				result = append(result, b...)
			}
		}
	}

	result = append(result, '}')

	return string(result), nil
}

func FromJSONWithString(data string) (theResult interface{}, err error) {
	return FromJSONWithBytes([]byte(data))
}

func FromJSONWithBytes(dataBytes []byte) (theResult interface{}, err error) {
	dec := json.NewDecoder(bytes.NewReader(dataBytes))
	return FromJSONWithDecoder(dec)
}

func FromJSONWithIOReader(reader io.Reader) (theResult interface{}, err error) {
	dec := json.NewDecoder(reader)
	return FromJSONWithDecoder(dec)
}

func FromJSONWithDecoder(dec *json.Decoder) (theResult interface{}, err error) {
	dec.UseNumber()

	// must open with a delim token '{'
	t, err := dec.Token()
	if err != nil {
		ThrowException(cErrors.Cause(err))
		return nil, err
	}

	if delim, ok := t.(json.Delim); !ok || (delim != '{' && delim != '[') {
		return nil, fmt.Errorf("invalid JSON object")
	}

	theResult, err = handledelim(t, dec)
	if err != nil {
		ThrowException(cErrors.Cause(err))
		return nil, err
	}

	t, err = dec.Token()
	if err != io.EOF {
		err = cErrors.New(fmt.Sprintf("expect end of JSON object but got more token: %T: %v or err: %v", t, t, err))
		ThrowException(err)
		return nil, err
	}

	return theResult, nil
}

func parseobject(hashmap *CypressHashMap, dec *json.Decoder) (err error) {
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

		hashmap.AddQueryArgument(key, value)

		/*
			// om.keys = append(om.keys, key)
			om.keys[key] = om.l.PushBack(key)
			om.m[key] = value*/
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

func parsearray(dec *json.Decoder) (result interface{}, err error) {
	var t json.Token

	var arraySlice []interface{}
	arrayList := NewList()

	shouldReturnList, shouldReturnSlice := false, false
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

		switch value.(type) {
		case *CypressHashMap:
			{
				arrayList.AddNewRecord(value.(*CypressHashMap))
				shouldReturnList = true
				shouldReturnSlice = false
			}
		case *CypressArrayList:
			{
				arrayList = value.(*CypressArrayList)
				shouldReturnList = true
				shouldReturnSlice = false
			}
		default:
			{
				arraySlice = append(arraySlice, value)
				shouldReturnList = false
				shouldReturnSlice = true
			}
		}
	}

	t, err = dec.Token()
	if err != nil {
		return
	}
	if delim, ok := t.(json.Delim); !ok || delim != ']' {
		err = fmt.Errorf("expect JSON array close with ']'")
		return
	}

	if shouldReturnSlice {
		return arraySlice, nil
	}

	if shouldReturnList {
		return arrayList, nil
	}

	return
}

func handledelim(t json.Token, dec *json.Decoder) (res interface{}, err error) {
	if delim, ok := t.(json.Delim); ok {
		switch delim {
		case '{':
			hashMap := NewMap()
			err = parseobject(hashMap, dec)
			if err != nil {
				return
			}
			return hashMap, nil
		case '[':
			var value interface{}
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
}
