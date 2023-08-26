package securityutils

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"github.com/codecypress/go-ancillary-utils/cypressutils"
	cErrors "github.com/pkg/errors"
	"golang.org/x/crypto/bcrypt"
)

func Base64Encode(clearText string) string {
	return base64.StdEncoding.EncodeToString([]byte(clearText))
}

func Base64EncodeBytes(bytea []byte) string {
	return base64.StdEncoding.EncodeToString(bytea)
}

func Base64Decode(encodedStr string) string {
	encStringByteArray, err := base64.StdEncoding.DecodeString(encodedStr)

	if err != nil {
		cypressutils.ThrowException(cErrors.Cause(err))
		return ""
	}
	return string(encStringByteArray)
}

func Base64DecodeBytes(bytea []byte) string {
	return base64.StdEncoding.EncodeToString(bytea)
}

func SHA256(cleartext string) string {
	sum := sha256.Sum256([]byte(cleartext))
	return fmt.Sprintf("%x", sum)
}

func BcryptHashPassword(password string) string {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), 5)
	if err != nil {
		cypressutils.ThrowException(cErrors.Cause(err))
		return ""
	}
	return string(bytes)
}

func BcryptIsValidPasswordHash(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	if err != nil {
		cypressutils.ThrowException(cErrors.Cause(err))
	}

	return err == nil
}
