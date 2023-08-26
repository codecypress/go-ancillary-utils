package cypressutils

import (
	"bytes"
	"crypto/des"
	"encoding/base64"
	"errors"
)

// e4L3Jp@?&G2+G7q5573HMvjC
// !&?GawXS+91+g8+uJ+ql4&3!
var _runeKey3DES = []rune{'!', '&', '?', 'G', 'a', 'w', 'X', 'S', '+', '9', '1', '+', 'g', '8', '+', 'u', 'J', '+', 'q', 'l', '4', '&', '3', '!'}

/*func Encrypt3DES(plaintext string, keyString ...string ) (string, error) {
	desCipher, err := des.NewTripleDESCipher(getKeyTripleDES(keyString))
	if err != nil {
		return "", err
	}

	out :=  make([]byte, len(plaintext))
	desCipher.Encrypt(out, []byte(plaintext))

	fmt.Println("Byte Form", out)

	return base64.StdEncoding.EncodeToString(out), nil
}

func Decrypt3DES(encryptedText string, keyString ...string) (string, error) {
	ciphertext, _ := base64.StdEncoding.DecodeString(encryptedText)

	//ciphertext, _ := hex.DecodeString(encryptedText)
	desCipher, err := des.NewTripleDESCipher(getKeyTripleDES(keyString))
	if err != nil {
		return "", err
	}
	plain := make([]byte, len(ciphertext))
	desCipher.Decrypt(plain, ciphertext)
	return string(plain[:]), nil
}*/

func EncryptDES(plainText string, keyString ...string) (string, error) {
	src := []byte(plainText)

	block, err := des.NewTripleDESCipher(getKeyDES(keyString, 24))
	if err != nil {
		return "", err
	}
	bs := block.BlockSize()
	src = pKCS5Padding(src, bs)
	if len(src)%bs != 0 {
		return "", errors.New("need a multiple of the block size")
	}
	out := make([]byte, len(src))
	dst := out
	for len(src) > 0 {
		block.Encrypt(dst, src[:bs])
		src = src[bs:]
		dst = dst[bs:]
	}
	return base64.StdEncoding.EncodeToString(out), nil
}

func DecryptDES(encryptedText string, keyString ...string) (string, error) {
	encStringByteArray, _ := base64.StdEncoding.DecodeString(encryptedText)

	block, err := des.NewTripleDESCipher(getKeyDES(keyString, 24))
	if err != nil {
		return "", err
	}
	out := make([]byte, len(encStringByteArray))
	dst := out
	bs := block.BlockSize()
	if len(encStringByteArray)%bs != 0 {
		return "", errors.New("crypto/cipher: input not full blocks")
	}
	for len(encStringByteArray) > 0 {
		block.Decrypt(dst, encStringByteArray[:bs])
		encStringByteArray = encStringByteArray[bs:]
		dst = dst[bs:]
	}
	//out = zeroUnPadding(out)
	out = pKCS5UnPadding(out)
	return string(out), nil
}

/*
func DecryptDES(encryptedText string, keyString ...string) (string, error) {
	ciphertext, _ := base64.StdEncoding.DecodeString(encryptedText)

	//ciphertext, _ := hex.DecodeString(encryptedText)
	desCipher, err := des.NewTripleDESCipher(getKeyDES(keyString,24))
	if err != nil {
		return "", err
	}
	plain := make([]byte, len(ciphertext))
	desCipher.Decrypt(plain, ciphertext)
	return string(plain), nil
}*/

func encryptLegacyDES(plainText string, keyString ...string) (string, error) {
	src := []byte(plainText)

	block, err := des.NewCipher(getKeyDES(keyString, 8))
	if err != nil {
		return "", err
	}
	bs := block.BlockSize()
	src = zeroPadding(src, bs)
	if len(src)%bs != 0 {
		return "", errors.New("need a multiple of the block size")
	}
	out := make([]byte, len(src))
	dst := out
	for len(src) > 0 {
		block.Encrypt(dst, src[:bs])
		src = src[bs:]
		dst = dst[bs:]
	}
	return base64.StdEncoding.EncodeToString(out), nil
}

func decryptLegacyDES(encryptedText string, keyString ...string) (string, error) {
	encStringByteArray, _ := base64.StdEncoding.DecodeString(encryptedText)

	block, err := des.NewCipher(getKeyDES(keyString, 8))
	if err != nil {
		return "", err
	}
	out := make([]byte, len(encStringByteArray))
	dst := out
	bs := block.BlockSize()
	if len(encStringByteArray)%bs != 0 {
		return "", errors.New("crypto/cipher: input not full blocks")
	}
	for len(encStringByteArray) > 0 {
		block.Decrypt(dst, encStringByteArray[:bs])
		encStringByteArray = encStringByteArray[bs:]
		dst = dst[bs:]
	}
	out = zeroUnPadding(out)
	return string(out), nil
}

func getKeyDES(keyString []string, keyLength int) []byte {
	tempKey := string(_runeKey3DES)

	if keyString != nil {
		tempKey = keyString[0]
	}

	return []byte(tempKey)[:keyLength]
}

func pKCS5Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

func pKCS5UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}

func zeroPadding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{0}, padding)
	return append(ciphertext, padtext...)
}

func zeroUnPadding(origData []byte) []byte {
	return bytes.TrimFunc(origData,
		func(r rune) bool {
			return r == rune(0)
		})
}
