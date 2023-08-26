package cypressutils

import (
	"github.com/google/uuid"
	cErrors "github.com/pkg/errors"
	"strconv"
	"strings"
	"time"
)

const salt = "abcdefghijklmnopqrstuvwxyz@#$!%^&*(*)1234567890"

func GetUUID() string {
	id, err := uuid.NewRandom()
	if err != nil {
		ThrowException(cErrors.Cause(err))
		return GetCurrentUnixTimeStamp()
	}
	return id.String()
}

func GetCurrentUnixTimeStamp() string {
	return strconv.Itoa(int(time.Now().UnixNano()))
}

func GetScedarUUID() string {
	return SHA256(GetCurrentUnixTimeStamp() + salt + GetUUID())
}

func GetAccessToken() string {
	return strings.ToLower(GetScedarUUID())
}
