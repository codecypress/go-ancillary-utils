package cypressutils

import (
	"fmt"
	cErrors "github.com/pkg/errors"
	"io"
	"io/ioutil"
	"mime/multipart"
	"os"
	"regexp"
	"strings"
)

func ReadFromFile(strFilePath string) (string, error) {
	b, err := os.ReadFile(strFilePath)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

func ReadFromFileToBytes(strFilePath string) ([]byte, error) {
	b, err := ioutil.ReadFile(strFilePath)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func WriteToFile(strFilePath string, data interface{}) (err error) {
	file, err := os.OpenFile(strFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	defer file.Close()
	if err != nil {
		return
	}

	if _, err := file.Write([]byte(fmt.Sprintf("%v", data))); err != nil {
		return err
	}

	return nil

}

func GetFileExtension(fileName string) string {
	i := strings.LastIndex(fileName, ".")
	if i > 0 {
		return fileName[i+1:]
	}
	return ""
}

func DeleteFile(filePath string) error {
	return os.Remove(filePath)
}

type AllowedImageExtension string

const (
	IMG_EXT_PNG  AllowedImageExtension = "png"
	IMG_EXT_JPG  AllowedImageExtension = "jpg"
	IMG_EXT_JPEG AllowedImageExtension = "jpeg"
	IMG_EXT_GIT  AllowedImageExtension = "gif"
	IMG_EXT_BMP  AllowedImageExtension = "bmp"
	IMG_EXT_SVG  AllowedImageExtension = "svg"
)

func SaveUploadedImage(file *multipart.FileHeader, fileName, filePath string, allowedExtensions ...AllowedImageExtension) (string, error) {
	src, err := file.Open()
	if err != nil {
		ThrowException(cErrors.Cause(err))
		return "", err
	}

	defer src.Close()

	extension := GetFileExtension(file.Filename)

	if allowedExtensions == nil {
		allowedExtensions = append(allowedExtensions, IMG_EXT_PNG, IMG_EXT_JPG, IMG_EXT_JPEG, IMG_EXT_GIT, IMG_EXT_BMP, IMG_EXT_SVG)
	}

	isValidExt := false
	for _, allowedExtension := range allowedExtensions {
		if strings.EqualFold(string(allowedExtension), extension) {
			isValidExt = true
			break
		}
	}

	if !isValidExt {
		err := cErrors.New("Improper file extension. Image with Extension '" + extension + "' not accepted")
		ThrowException(err)
		return "", err
	}

	reg1 := regexp.MustCompile("\\s{2,}")
	reg2 := regexp.MustCompile("[^A-Za-z0-9]")

	fileName = strings.TrimSpace(fileName)
	fileName = reg1.ReplaceAllString(fileName, " ")
	fileName = reg2.ReplaceAllString(fileName, "_")
	fileName = strings.ToLower(fileName)

	newFileName := filePath + "/" + fileName + "." + extension

	// Destination
	dst, err := os.Create(newFileName)
	if err != nil {
		ThrowException(cErrors.Cause(err))
		return "", err
	}
	defer dst.Close()

	// Copy
	if _, err = io.Copy(dst, src); err != nil {
		ThrowException(cErrors.Cause(err))
		return "", err
	}

	return fileName + "." + extension, nil
}

type AllowedFileExtension string

const (
	FILE_EXT_XLS  AllowedFileExtension = "xls"
	FILE_EXT_XLSX AllowedFileExtension = "xlsx"
	FILE_EXT_PDF  AllowedFileExtension = "pdf"
	FILE_EXT_DOC  AllowedFileExtension = "doc"
	FILE_EXT_DOCX AllowedFileExtension = "docx"
	FILE_EXT_PPT  AllowedFileExtension = "ppt"
	FILE_EXT_PPTX AllowedFileExtension = "pptx"
	FILE_EXT_TXT  AllowedFileExtension = "txt"
	FILE_EXT_CSV  AllowedFileExtension = "csv"
)

func SaveUploadedFile(file multipart.FileHeader, fileName, filePath string, allowedExtensions ...AllowedFileExtension) (string, error) {
	src, err := file.Open()
	if err != nil {
		ThrowException(cErrors.Cause(err))
		return "", err
	}

	defer src.Close()

	extension := GetFileExtension(file.Filename)

	if allowedExtensions == nil {
		allowedExtensions = append(allowedExtensions, FILE_EXT_XLS, FILE_EXT_XLSX, FILE_EXT_PDF, FILE_EXT_DOC, FILE_EXT_DOCX, FILE_EXT_PPT, FILE_EXT_PPTX, FILE_EXT_TXT, FILE_EXT_CSV)
	}

	isValidExt := false
	for _, allowedExtension := range allowedExtensions {
		if strings.EqualFold(string(allowedExtension), extension) {
			isValidExt = true
			break
		}
	}

	if !isValidExt {
		err := cErrors.New("Improper file extension. File with Extension '" + extension + "' not accepted")
		ThrowException(err)
		return "", err
	}

	reg1 := regexp.MustCompile("\\s{2,}")
	reg2 := regexp.MustCompile("[^A-Za-z0-9]")

	fileName = strings.TrimSpace(fileName)
	fileName = reg1.ReplaceAllString(fileName, " ")
	fileName = reg2.ReplaceAllString(fileName, "_")
	fileName = strings.ToLower(fileName)

	newFileName := filePath + "/" + fileName + "." + extension

	// Destination
	dst, err := os.Create(newFileName)
	if err != nil {
		ThrowException(cErrors.Cause(err))
		return "", err
	}
	defer dst.Close()

	// Copy
	if _, err = io.Copy(dst, src); err != nil {
		ThrowException(cErrors.Cause(err))
		return "", err
	}

	return newFileName, nil
}
