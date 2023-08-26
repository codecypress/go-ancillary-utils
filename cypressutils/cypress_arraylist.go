package cypressutils

import (
	"fmt"
	"github.com/codecypress/go-ancillary-utils/miscellaneous"
	"math/rand"
	"strconv"
	"strings"
)

type CypressArrayList struct {
	hashmaps  []*CypressHashMap
	tableName string
}

func NewList() *CypressArrayList {
	return &CypressArrayList{
		hashmaps:  []*CypressHashMap{},
		tableName: "[Undeclared]",
	}
}

func (list *CypressArrayList) Size() int {
	return len(list.hashmaps)
}

func (list *CypressArrayList) ShuffleRecords() {
	rand.Shuffle(len(list.hashmaps), func(i, j int) {
		list.hashmaps[i], list.hashmaps[j] = list.hashmaps[j], list.hashmaps[i]
	})
}

func (list *CypressArrayList) IsEmpty() bool {
	return len(list.hashmaps) == 0
}

func (list *CypressArrayList) AddNewRecord(hashMap *CypressHashMap) {
	list.hashmaps = append(list.hashmaps, hashMap)
}

func (list *CypressArrayList) GetRecord(index int) *CypressHashMap {
	if index >= list.Size() {
		return nil
	}
	return list.hashmaps[index]
}

func (list *CypressArrayList) PrintRecordsTabular(fieldWidthMapSlice ...map[string]int) {
	fieldWidthMap := map[string]int{}

	if fieldWidthMapSlice != nil {
		fieldWidthMap = fieldWidthMapSlice[0]
	}

	if !list.IsEmpty() {
		fmt.Println("\nTABLE: " + list.tableName)
		fmt.Println("********************************")

		listSize := list.Size()
		tempHashMap := list.hashmaps[0]

		for pair := tempHashMap.GetData().Oldest(); pair != nil; pair = pair.Next() {
			field := fmt.Sprintf("%v", pair.Key)

			width := 30
			if temp, exists := fieldWidthMap[field]; exists {
				width = temp
			}

			field = strings.ToUpper(field)
			fmt.Printf("%-"+strconv.Itoa(width)+"s", miscellaneous.AbbreviateString(field, width))
		}

		fmt.Println()

		for index := 0; index < listSize; index++ {
			hashmap := list.hashmaps[index]

			for pair := hashmap.GetData().Oldest(); pair != nil; pair = pair.Next() {
				field := fmt.Sprintf("%v", pair.Key)

				width := 30
				if temp, exists := fieldWidthMap[field]; exists {
					width = temp
				}

				value := ""
				if pair.Value != nil {
					value = fmt.Sprintf("%v", pair.Value)
				} else {
					for i := 0; i < width; i++ {
						value += " "
					}
				}

				fmt.Printf("%-"+strconv.Itoa(width)+"s", miscellaneous.AbbreviateString(value, width))
			}
			fmt.Println()
		}

		fmt.Println("\n*******************************")
		fmt.Println("TOTAL RECORDS =", list.Size())

	} else {
		fmt.Println("No record present")
	}
}

func (list *CypressArrayList) PrintRecordsVerticallyLabelled() {
	if !list.IsEmpty() {
		fmt.Println("\nTABLE: " + list.tableName)
		fmt.Println("********************************")

		listSize := list.Size()
		for index := 0; index < listSize; index++ {
			hashmap := list.hashmaps[index]
			longestField := hashmap.GetLongestField()

			for pair := hashmap.GetData().Oldest(); pair != nil; pair = pair.Next() {
				field := fmt.Sprintf("%v", pair.Key)

				value := ""
				if pair.Value != nil {
					value = fmt.Sprintf("%v", pair.Value)
				}

				fmt.Printf("%"+strconv.Itoa(longestField)+"s: %s", field, value)
				fmt.Println()
			}
			if listSize > 1 {
				fmt.Println("--------------------------------------------------------\n")
			}
		}

		fmt.Println("********************************")
		fmt.Println("TOTAL RECORDS =", list.Size())
	} else {
		fmt.Println("No record present")
	}
}

func (list *CypressArrayList) GetValuesForColumn(columnName string) (valuesList []interface{}) {
	listSize := list.Size()
	for index := 0; index < listSize; index++ {
		hashmap := list.hashmaps[index]
		valuesList = append(valuesList, hashmap.GetValue(columnName))
	}
	return
}

func (list *CypressArrayList) GetValuesForColumnAsMap(columnName string) (valuesMap map[string]interface{}) {
	valuesMap = map[string]interface{}{}

	listSize := list.Size()
	for index := 0; index < listSize; index++ {
		hashmap := list.hashmaps[index]
		valuesMap[hashmap.GetStringValue(columnName)] = nil
	}
	return
}

func (list *CypressArrayList) SetCommonFieldAndValue(field string, value interface{}) {
	listSize := list.Size()
	for index := 0; index < listSize; index++ {
		list.hashmaps[index].PutValue(field, value)
	}
}

func (list *CypressArrayList) CopyFrom(otherList *CypressArrayList) {
	listSize := otherList.Size()
	for index := 0; index < listSize; index++ {
		list.AddNewRecord(otherList.hashmaps[index].CloneMe())
	}
}

func (list *CypressArrayList) CloneMe() (otherList *CypressArrayList) {
	otherList = NewList()
	listSize := list.Size()
	for index := 0; index < listSize; index++ {
		otherList.AddNewRecord(list.hashmaps[index].CloneMe())
	}
	return
}
