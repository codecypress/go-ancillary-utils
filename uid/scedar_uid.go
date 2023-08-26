package uid

import (
	"github.com/codecypress/go-ancillary-utils/miscellaneous"
	"regexp"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
const uid_size = 15

var numberOfCodePoints = len(letters)
var uidPattern = regexp.MustCompile("^[a-zA-Z]{1}[a-zA-Z0-9]{10}$")

func GenerateUid() string {
	return GenerateUidWithSize(uid_size)
}

func GenerateUidWithSize(size int) string {
	randomChars := make([]uint8, size)
	for i := 0; i < size; i++ {
		/*fmt.Println("Num:", num)
		fmt.Println("Letter:", letters[num])*/
		randomChars[i] = letters[miscellaneous.RandomNum(numberOfCodePoints)]
	}
	return string(randomChars)
}

func IsValidUID(uidStr string) bool {
	return uidPattern.MatchString(uidStr)
}
