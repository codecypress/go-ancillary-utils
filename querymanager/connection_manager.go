package querymanager

import (
	"database/sql"
	"fmt"
	"github.com/codecypress/go-ancillary-utils/miscellaneous"
	"github.com/codecypress/go-ancillary-utils/xmlutils"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	cErrors "github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"strings"
)

type DbTypes string

const (
	DB_TYPE_MSQL          DbTypes = "mysql"
	DB_TYPE_MICROSOFT_SQL DbTypes = ""
	DB_TYPE_POSTGRESQL    DbTypes = "postgres"
)

// ConDSN Data Source Name
type ConDSN struct {
	organizationId                                                     string
	databaseServer                                                     DbTypes
	databaseName, databaseHost, userName, password, connectionMetadata string
	port                                                               int
}

type ConnectionsDSNs struct {
	conDSNs map[string]*ConDSN
}

var connectionsDSNs *ConnectionsDSNs = &ConnectionsDSNs{
	conDSNs: make(map[string]*ConDSN),
}

func SetupDSNs() {
	databaseServer := DB_TYPE_POSTGRESQL
	masterUserName, _ := xmlutils.ConfGetDBUserName()
	masterPassword, _ := xmlutils.ConfGetDBPassword()
	masterDatabaseName, _ := xmlutils.ConfGetDatabaseName()
	masterDatabaseHost, _ := xmlutils.ConfGetDBHost()
	masterPort, _ := xmlutils.ConfGetDBPort()
	maxIdleConns, _ := xmlutils.ConfSlingRingInitialPoolSize()
	maxOpenConns, _ := xmlutils.ConfSlingRingMaxPoolSize()

	masterDSNURL := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		masterDatabaseHost, masterPort, masterUserName, masterPassword, masterDatabaseName)
	//masterDSNURLMasked := masterUserName + ":***@tcp(" + masterDatabaseHost + ":" + strconv.Itoa(masterPort) + ")/" + masterDatabaseName

	fmt.Println("\n ---------------------<", "database", ">---------------------")
	fmt.Println("", miscellaneous.PadStringToPrintInConsole(strings.ToUpper(masterDatabaseName), 54, " "))
	fmt.Println("", miscellaneous.PadStringToPrintInConsole("------[ Creating connection pool... ]------", 54, " "))
	fmt.Println(" Database Server   : ", databaseServer)
	fmt.Println(" Database Username : ", masterUserName)
	//fmt.Println(" Connection URL:", masterDSNURLMasked)

	masterDb, err := sql.Open("postgres", masterDSNURL)
	defer masterDb.Close()

	if err != nil {
		logrus.Fatal(err)
	}

	err = masterDb.Ping()
	if err != nil {
		logrus.Fatal(err)
	}

	//masterDb.SetConnMaxLifetime(time.Minute * 3)
	masterDb.SetMaxIdleConns(maxIdleConns)
	masterDb.SetMaxOpenConns(maxOpenConns)

	masterConDSN := &ConDSN{
		organizationId:     "-1L",
		databaseServer:     databaseServer,
		databaseName:       masterDatabaseName,
		databaseHost:       masterDatabaseHost,
		userName:           masterUserName,
		password:           masterPassword,
		connectionMetadata: "",
		port:               masterPort,
	}

	connectionsDSNs.conDSNs["-1L"] = masterConDSN

	//TODO: FETCH CONNECTIONS AND POPULATE HERE
}

func connectToDatabase(databaseType DbTypes, databaseName, databaseHost, userName, password, connectionMetadata string, port int) (*sql.DB, error) {

	masterDSNURL := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		databaseHost, port, userName, password, databaseName)

	return sql.Open(string(databaseType), masterDSNURL)
}

func GetConnection(organizationId string) (*sql.DB, error) {
	conDSN, exists := connectionsDSNs.conDSNs[organizationId]

	if !exists {
		err := cErrors.New("No Connection Found where Organization Id = '" + organizationId + "'")

		logrus.Error(err.Error())
		return nil, err
	}

	databaseServer := conDSN.databaseServer
	userName := conDSN.userName
	password := conDSN.password
	databaseName := conDSN.databaseName
	databaseHost := conDSN.databaseHost
	port := conDSN.port
	//connectionMetadata := conDSN.connectionMetadata

	masterDSNURL := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		databaseHost, port, userName, password, databaseName)

	return sql.Open(string(databaseServer), masterDSNURL)
}

func GetConDSN(organizationId string) *ConDSN {
	conDSN, exists := connectionsDSNs.conDSNs[organizationId]
	if !exists {
		err := cErrors.New("No Connection DSN Found where Organization Id = '" + organizationId + "'")
		logrus.Error(err.Error())
		return nil
	}
	return conDSN
}

func (conDSN *ConDSN) GetOrganizationId() string {
	return conDSN.organizationId
}

func (conDSN *ConDSN) GetDatabaseName() string {
	return conDSN.databaseName
}

func (conDSN *ConDSN) GetDatabaseHost() string {
	return conDSN.databaseHost
}

func (conDSN *ConDSN) GetUsername() string {
	return conDSN.userName
}

func (conDSN *ConDSN) GetPassword() string {
	return conDSN.password
}

func (conDSN *ConDSN) GetConnectionMetadata() string {
	return conDSN.connectionMetadata
}

func (conDSN *ConDSN) GetDatabaseServer() DbTypes {
	return conDSN.databaseServer
}

func (conDSN *ConDSN) GetPort() int {
	return conDSN.port
}
