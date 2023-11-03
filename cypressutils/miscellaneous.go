package cypressutils

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	cErrors "github.com/pkg/errors"
	"io/ioutil"
	"jaytaylor.com/html2text"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func GetWorkingDir() (workingDir string) {
	workingDir, err := os.Getwd()

	if err != nil {
		PanicError(err)
	}
	return
}

func PanicError(e error) {
	if e != nil {
		panic(e)
	}
}

func MinOf(vars ...int) int {
	min := vars[0]
	for _, i := range vars {
		if min > i {
			min = i
		}
	}

	return min
}

func MaxOf(vars ...int) int {
	max := vars[0]

	for _, i := range vars {
		if max < i {
			max = i
		}
	}

	return max
}

func AbbreviateString(str string, noOfChars int) string {
	if len(strings.TrimSpace(str)) > 0 {
		str = str[:MinOf(len(str), noOfChars)]
		length := len(str)
		if length == noOfChars {
			str = str[:length-3] + "..."
		}
	}
	return str
}

func PadStringToPrintInConsole(originalString string, consoleOutputLength int, padChar string) string {
	origiLen := len(originalString)
	if consoleOutputLength <= origiLen {
		return originalString
	}

	var buf bytes.Buffer
	tempInt := consoleOutputLength - origiLen
	firstHalf := (tempInt / 2) + 1
	lastHalf := tempInt - firstHalf

	for i := 0; i < firstHalf; i++ {
		buf.WriteString(padChar)
	}
	buf.WriteString(originalString)
	for i := 0; i < lastHalf; i++ {
		buf.WriteString(padChar)
	}
	return buf.String()
}

func IsIdentifierStart(c uint8) bool {
	reg := regexp.MustCompile("[a-zA-Z$_]")
	return reg.MatchString(string(c))
}

func IsIdentifierPart(c uint8) bool {
	reg := regexp.MustCompile("[a-z0-9A-Z$_]")
	return reg.MatchString(string(c))
}

func RandomNumInRange(min int, max int) int {
	return rand.Intn(max-min+1) + min
}

func RandomNum(max int) int {
	return rand.Intn(max)
}

func ConvertFromCamelCaseToSnakeCase(camelCase string) string {
	reg := regexp.MustCompile("([a-z])([A-Z]+)")
	return reg.ReplaceAllString(camelCase, "$1_$2")
}

func Int64ToBytes(i int64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

func BytesToInt64(buf []byte) int64 {
	aByteToInt, _ := strconv.ParseInt(string(buf), 10, 64)
	return aByteToInt
	//return int64(binary.BigEndian.Uint64(buf))
}

func ImageToBase64(imagePath string) (string, error) {
	byteArr, err := ioutil.ReadFile(imagePath)
	if err != nil {
		ThrowException(cErrors.Cause(err))
		return "", err
	}
	return base64.StdEncoding.EncodeToString(byteArr), nil
}

func Html2Text(html string) string {
	text, err := html2text.FromString(html, html2text.Options{PrettyTables: true})
	if err != nil {
		ThrowException(cErrors.Cause(err))
		return html
	}
	return text
}

func GetCallerFilename(filePath string) string {
	if strings.Contains(filePath, "/") {
		return filePath[strings.LastIndex(filePath, "/")+1:]
	} else {
		return filePath[strings.LastIndex(filePath, "\\")+1:]
	}
}

func ToString(data interface{}) string {
	return fmt.Sprintf("%v", data)
}
