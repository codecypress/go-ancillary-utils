package cypressutils

import (
	"fmt"
	"github.com/antchfx/xmlquery"
	cErrors "github.com/pkg/errors"
	"os"
	"strconv"
	"strings"
)

func GetConfFilePath() string {
	return GetWorkingDir() + "/conf.xml"
}

func GetXSDConfFilePath() string {
	return GetWorkingDir() + "/conf.schema.xsd"
}

var confCacheableParameters = make(map[string]interface{})

const (
	ENCRYPTED = "ENCRYPTED"
	CLEARTEXT = "CLEARTEXT"

	XML_PATH_TO_ECHO_SERVER_REST_BASE_PATH          = "/API/ECHO_SERVER/BASE_PATH/@REST"
	XML_PATH_TO_ECHO_SERVER_PORTAL_PATH             = "/API/ECHO_SERVER/BASE_PATH/@PORTAL"
	XML_PATH_TO_ECHO_SERVER_ESS_PATH_ACTIVATION     = "/API/ECHO_SERVER/ESS_URLS/ACTIVATION/@URL"
	XML_PATH_TO_ECHO_SERVER_ESS_PATH_LOGIN          = "/API/ECHO_SERVER/ESS_URLS/LOGIN/@URL"
	XML_PATH_TO_ECHO_SERVER_ESS_PATH_PASSWORD_RESET = "/API/ECHO_SERVER/ESS_URLS/PASSWORD_RESET/@URL"
	XML_PATH_TO_ECHO_SERVER_REST_HOST               = "/API/ECHO_SERVER/HOST/@REST"
	XML_PATH_TO_ECHO_SERVER_REST_PORT               = "/API/ECHO_SERVER/PORT/@REST"
	XML_PATH_TO_ECHO_SERVER_REGISTRY_PORT           = "/API/ECHO_SERVER/PORT/@REGISTRY"
	XML_PATH_TO_ECHO_SERVER_NOTIFICATIONS_PORT      = "/API/ECHO_SERVER/PORT/@NOTIFICATIONS"
	XML_PATH_TO_ECHO_SERVER_DEFAULT_ORG_CODE        = "/API/ECHO_SERVER/ORGANIZATIONS/@DEFAULT_CODE"
	XML_PATH_TO_UNDERTOW_IO_THREAD_POOL             = "/API/ECHO_SERVER/UNDERTOW/@IO_THREAD_POOL"
	XML_PATH_TO_UNDERTOW_WORKER_THREAD_POOL         = "/API/ECHO_SERVER/UNDERTOW/@WORKER_THREAD_POOL"
	XML_PATH_TO_AUTH_ACCESS_TOKEN_TIMEOUT           = "/API/AUTHENTICATION/ACCESS_TOKEN/TIMEOUT"
	XML_PATH_TO_AUTH_ACCESS_TOKEN_TIMEOUT_TIME_UNIT = "/API/AUTHENTICATION/ACCESS_TOKEN/TIMEOUT/@TIME_UNIT"
	XML_PATH_TO_NOTIFICATIONS_PING_AFTER            = "/API/ECHO_SERVER/NOTIFICATION_SERVER/PING_AFTER"
	XML_PATH_TO_NOTIFICATIONS_PING_AFTER_TIME_UNIT  = "/API/ECHO_SERVER/NOTIFICATION_SERVER/PING_AFTER/@TIME_UNIT"
	XML_PATH_TO_INITIALIZE_DATABASE                 = "/API/DB/INITIALIZE"
	XML_PATH_TO_SERVER                              = "/API/DB/SERVER"
	XML_PATH_TO_DB_HOST                             = "/API/DB/HOST"
	XML_PATH_TO_DB_PORT                             = "/API/DB/PORT"
	XML_PATH_TO_DATABASE_NAME                       = "/API/DB/DATABASE_NAME"
	XML_PATH_TO_DB_PASSWORD_TYPE                    = "/API/DB/PASSWORD/@TYPE"
	XML_PATH_TO_DB_USERNAME_TYPE                    = "/API/DB/USERNAME/@TYPE"
	XML_PATH_TO_DB_NAME_TYPE                        = "/API/DB/DATABASE_NAME/@TYPE"
	XML_PATH_TO_CONN_METADATA                       = "/API/DB/CONN_METADATA"
	XML_PATH_TO_DB_USERNAME                         = "/API/DB/USERNAME"
	XML_PATH_TO_DB_PASSWORD                         = "/API/DB/PASSWORD"
	XML_PATH_TO_DB_SHOW_SQL                         = "/API/DB/SHOW_SQL"
	XML_PATH_TO_RESOURCES                           = "/API/DB/RESOURCES/@PATH"
	XML_PATH_TO_BACKUP_FOLDER                       = "/API/DB/BACKUP_FOLDER/@PATH"
	XML_PATH_TO_TABLE_COUNT                         = "/API/DB/@TBL_CNT"
	XML_PATH_TO_QUERY_MANAGER_PATH                  = "/API/DB/QUERY_MANAGER"
	XML_PATH_TO_QUERY_MANAGER_INSTANCE              = "/API/DB/QUERY_MANAGER/@INSTANCE"
	XML_PATH_TO_QUERY_MANAGER_JAR_NAME              = "/API/DB/QUERY_MANAGER/@NAME"
	XML_PATH_TO_QUERY_MANAGER_PATH_TYPE             = "/API/DB/QUERY_MANAGER/@PATH_TYPE"
	SLING_RING_INITIAL_POOL_SIZE                    = "/API/DB/SLING_RING/INITIAL_POOL_SIZE/@VALUE"
	SLING_RING_MAXIMUM_POOL_SIZE                    = "/API/DB/SLING_RING/MAXIMUM_POOL_SIZE/@VALUE"
	SLING_RING_EXTRA_CONNS_SIZE                     = "/API/DB/SLING_RING/EXTRA_CONNS_SIZE/@VALUE"
	SLING_RING_FIND_FREE_CONN_AFTER                 = "/API/DB/SLING_RING/FIND_FREE_CONN_AFTER/@VALUE"
	SLING_RING_FIND_FREE_CONN_AFTER_TIME_UNIT       = "/API/DB/SLING_RING/FIND_FREE_CONN_AFTER/@TIME_UNIT"
	SLING_RING_DOWNSIZE_AFTER                       = "/API/DB/SLING_RING/DOWNSIZE_AFTER/@VALUE"
	SLING_RING_DOWNSIZE_AFTER_TIME_UNIT             = "/API/DB/SLING_RING/DOWNSIZE_AFTER/@TIME_UNIT"
	SLING_RING_PING_AFTER                           = "/API/DB/SLING_RING/DOWNSIZE_AFTER/@VALUE"
	SLING_RING_PING_AFTER_TIME_UNIT                 = "/API/DB/SLING_RING/DOWNSIZE_AFTER/@TIME_UNIT"
	XML_PATH_TO_LOGGING_FORMAT                      = "/API/LOGGING/@FORMAT"
)

func ConfFileExists() {
	if _, err := os.Stat(GetConfFilePath()); os.IsNotExist(err) {
		if err != nil {
			PrintFatallnNoColor("Failed to Find the Conf file File Under Path: " + GetConfFilePath())
			os.Exit(1)
		}
	}

	if _, err := os.Stat(GetXSDConfFilePath()); os.IsNotExist(err) {
		if err != nil {
			PrintFatallnNoColor("Failed to Find the Conf XSD file File Under Path: " + GetXSDConfFilePath())
			os.Exit(1)
		}
	}
	cacheCacheableParameters()
}

func cacheCacheableParameters() {
	format, err := ConfGetLoggingFormat()
	if err != nil {
		PrintErrorlnNoColor("CONF: Failed to get logging format. Defaulted to 'FORMAT B'")
		format = "FORMAT B"
	}
	confCacheableParameters[XML_PATH_TO_LOGGING_FORMAT] = format
}

func ParseConfXMLFile(param interface{}) (*xmlquery.Node, error) {
	defer func() {
		if r := recover(); r != nil {
			err := cErrors.Cause(fmt.Errorf("PANIC: %v", r))
			ThrowException(err)
		}
	}()

	switch param.(type) {
	case *os.File:
		{
			file := param.(*os.File)
			defer file.Close()
			doc, err := xmlquery.Parse(file)
			if err != nil {
				PrintErrorlnNoColor(err.Error())
				return nil, err
			}

			return doc, nil
		}
	case string:
		{
			strFilePath := param.(string)

			reader, err := os.Open(strFilePath)

			if err != nil {
				PrintErrorlnNoColor(err.Error())
				return nil, err
			}

			defer reader.Close()

			doc, err := xmlquery.Parse(reader)

			if err != nil {
				PrintErrorlnNoColor(err.Error())
				return nil, err
			}

			return doc, nil
		}
	default:
		{
			err := cErrors.New(fmt.Sprintf("Unknown type %T. Acceptable arguments [File Pointer or Filepath]", param))
			PrintErrorlnNoColor(err.Error())
			return nil, err
		}
	}
}

func ConfUpdateXMLTag(strXPath string, value interface{}) error {
	doc, err := ParseConfXMLFile(GetConfFilePath())
	if err != nil {
		return err
	}

	strUpdatedConf, err := UpdateXMLTag(doc, strXPath, value)
	if err != nil {
		return err
	}

	strUpdatedConf = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + strUpdatedConf

	return WriteToFile(GetConfFilePath(), strUpdatedConf)
}

func ConfUpdateXMLTags(mapXPathsData map[string]interface{}) error {
	doc, err := ParseConfXMLFile(GetConfFilePath())
	if err != nil {
		return err
	}

	strUpdatedConf, err := UpdateXMLTags(doc, mapXPathsData)
	if err != nil {
		return err
	}

	strUpdatedConf = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + strUpdatedConf

	return WriteToFile(GetConfFilePath(), strUpdatedConf)
}

func ConfGetTagValue(strXPath string) (string, error) {
	doc, err := ParseConfXMLFile(GetConfFilePath())
	if err != nil {
		return "", err
	}
	return GetTagValue(doc, strXPath)
}

func ConfGetDatabaseName() (string, error) {
	dbName, err := ConfGetTagValue(XML_PATH_TO_DATABASE_NAME)
	if err != nil {
		return "", err
	}

	dbNameType, err := ConfGetTagValue(XML_PATH_TO_DB_NAME_TYPE)
	if err != nil {
		return "", err
	}

	if dbNameType == ENCRYPTED {
		return DecryptDES(dbName)
	} else {
		encryptedDBName, err := EncryptDES(dbName)
		if err != nil {
			return "", err
		}

		mapXPathsData := map[string]interface{}{
			XML_PATH_TO_DB_NAME_TYPE:  ENCRYPTED,
			XML_PATH_TO_DATABASE_NAME: encryptedDBName,
		}

		err = ConfUpdateXMLTags(mapXPathsData)

		//err = ConfUpdateXMLTag(XML_PATH_TO_DATABASE_NAME, encryptedDBName)

		if err != nil {
			return "", err
		}

		return dbName, nil
	}
}

func ConfGetDBUserName() (string, error) {
	dbUserName, err := ConfGetTagValue(XML_PATH_TO_DB_USERNAME)
	if err != nil {
		return "", err
	}

	dbUserNameType, err := ConfGetTagValue(XML_PATH_TO_DB_USERNAME_TYPE)
	if err != nil {
		return "", err
	}

	if dbUserNameType == ENCRYPTED {
		return DecryptDES(dbUserName)
	} else {
		encryptedDBUserName, err := EncryptDES(dbUserName)
		if err != nil {
			return "", err
		}

		mapXPathsData := map[string]interface{}{
			XML_PATH_TO_DB_USERNAME_TYPE: ENCRYPTED,
			XML_PATH_TO_DB_USERNAME:      encryptedDBUserName,
		}

		err = ConfUpdateXMLTags(mapXPathsData)

		if err != nil {
			return "", err
		}

		return dbUserName, nil
	}
}

func ConfGetDBPassword() (string, error) {
	dbPassword, err := ConfGetTagValue(XML_PATH_TO_DB_PASSWORD)
	if err != nil {
		return "", err
	}

	dbPasswordType, err := ConfGetTagValue(XML_PATH_TO_DB_PASSWORD_TYPE)
	if err != nil {
		return "", err
	}

	if dbPasswordType == ENCRYPTED {
		return DecryptDES(dbPassword)
	} else {
		encryptedDBPassword, err := EncryptDES(dbPassword)
		if err != nil {
			return "", err
		}

		mapXPathsData := map[string]interface{}{
			XML_PATH_TO_DB_PASSWORD_TYPE: ENCRYPTED,
			XML_PATH_TO_DB_PASSWORD:      encryptedDBPassword,
		}

		err = ConfUpdateXMLTags(mapXPathsData)

		if err != nil {
			return "", err
		}

		return dbPassword, nil
	}
}

func ConfGetDBHost() (string, error) {
	return ConfGetTagValue(XML_PATH_TO_DB_HOST)
}

func ConfGetLoggingFormat() (string, error) {
	if value, exists := confCacheableParameters[XML_PATH_TO_LOGGING_FORMAT]; exists {
		return fmt.Sprintf("%v", value), nil
	}
	return ConfGetTagValue(XML_PATH_TO_LOGGING_FORMAT)
}

func ConfGetDBPort() (int, error) {
	return ConfReadInt(XML_PATH_TO_DB_PORT)
}

func ConfGetRestAPIHTTPAddress() (string, error) {
	host, err := ConfGetTagValue(XML_PATH_TO_ECHO_SERVER_REST_HOST)
	if err != nil {
		return "", err
	}
	port, err := ConfGetTagValue(XML_PATH_TO_ECHO_SERVER_REST_PORT)
	if err != nil {
		return "", err
	}

	return host + ":" + port, nil
}

func ConfGetRestBasePath() (string, error) {
	return ConfGetTagValue(XML_PATH_TO_ECHO_SERVER_REST_BASE_PATH)
}

func ConfShowSQL() (bool, error) {
	return ConfReadBool(XML_PATH_TO_DB_SHOW_SQL)
}

func ConfSlingRingInitialPoolSize() (int, error) {
	return ConfReadInt(SLING_RING_INITIAL_POOL_SIZE)
}

func ConfSlingRingMaxPoolSize() (int, error) {
	return ConfReadInt(SLING_RING_MAXIMUM_POOL_SIZE)
}

func ConfSlingRingExtraConsSize() (int, error) {
	return ConfReadInt(SLING_RING_EXTRA_CONNS_SIZE)
}

func ConfSlingRingFindFreeConnAfter() (int, error) {
	return ConfReadInt(SLING_RING_FIND_FREE_CONN_AFTER)
}

func ConfSlingRingFindFreeConnAfterTimeUnit() (string, error) {
	return ConfGetTagValue(SLING_RING_FIND_FREE_CONN_AFTER_TIME_UNIT)
}

func ConfSlingRingDownSizeAfter() (int, error) {
	return ConfReadInt(SLING_RING_DOWNSIZE_AFTER)
}

func ConfSlingRingDownSizeAfterTimeUnit() (string, error) {
	return ConfGetTagValue(SLING_RING_DOWNSIZE_AFTER_TIME_UNIT)
}

func ConfSlingRingPingAfter() (int, error) {
	return ConfReadInt(SLING_RING_PING_AFTER)
}

func ConfSlingRingPingAfterTimeUnit() (string, error) {
	return ConfGetTagValue(SLING_RING_PING_AFTER_TIME_UNIT)
}

func ConfReadInt(strXpath string) (int, error) {
	strResult, err := ConfGetTagValue(strXpath)
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(strResult)
}

func ConfReadBool(strXpath string) (bool, error) {
	strResult, err := ConfGetTagValue(strXpath)
	if err != nil {
		return false, err
	}

	return strconv.ParseBool(strings.ToLower(strResult))
}

func ConfReadFloat32(strXpath string) (float64, error) {
	strResult, err := ConfGetTagValue(strXpath)
	if err != nil {
		return 0, err
	}

	return strconv.ParseFloat(strResult, 32)
}

func ConfReadFloat64(strXpath string) (float64, error) {
	strResult, err := ConfGetTagValue(strXpath)
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(strResult, 64)
}
