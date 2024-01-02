package cypressutils

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	cErrors "github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"strings"
)

type DbTypes string

const (
	DbTypeMysql        DbTypes = "mysql"
	DbTypeMicrosoftSql DbTypes = "sqlserver"
	DbTypePostgresql   DbTypes = "postgres"
	DbTypeOracle       DbTypes = "ora"
)

// ConDSN Data Source Name
type ConDSN struct {
	organizationId                                                     string
	databaseServer                                                     DbTypes
	databaseName, databaseHost, userName, password, connectionMetadata string
	port                                                               int
	maxIdleConnections, maxOpenConnections                             int
}

type ConnectionsDSNs struct {
	conDSNs map[string]*ConDSN
}

var connectionsDSNs *ConnectionsDSNs = &ConnectionsDSNs{
	conDSNs: make(map[string]*ConDSN),
}

func SetupDSNs() {
	databaseServer := DbTypePostgresql
	masterUserName, _ := ConfGetDBUserName()
	masterPassword, _ := ConfGetDBPassword()
	masterDatabaseName, _ := ConfGetDatabaseName()
	masterDatabaseHost, _ := ConfGetDBHost()
	masterPort, _ := ConfGetDBPort()
	maxIdleConns, _ := ConfSlingRingInitialPoolSize()
	maxOpenConns, _ := ConfSlingRingMaxPoolSize()

	masterDSNURL := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		masterDatabaseHost, masterPort, masterUserName, masterPassword, masterDatabaseName)
	//masterDSNURLMasked := masterUserName + ":***@tcp(" + masterDatabaseHost + ":" + strconv.Itoa(masterPort) + ")/" + masterDatabaseName

	fmt.Println("\n ---------------------<", "database", ">---------------------")
	fmt.Println("", PadStringToPrintInConsole(strings.ToUpper(masterDatabaseName), 54, " "))
	fmt.Println("", PadStringToPrintInConsole("------[ Creating connection pool... ]------", 54, " "))
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

func SetupDSN(organizationId string, conDSN *ConDSN) error {

	var masterDSNURL = ""

	switch conDSN.databaseServer {
	case DbTypePostgresql:
		masterDSNURL = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			conDSN.databaseHost, conDSN.port, conDSN.userName, conDSN.password, conDSN.databaseName)
	case DbTypeMicrosoftSql:
		masterDSNURL = fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s",
			conDSN.databaseHost, conDSN.userName, conDSN.password, conDSN.port, conDSN.databaseName)
	case DbTypeMysql:
		masterDSNURL = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", conDSN.userName, conDSN.password, conDSN.databaseHost, conDSN.port, conDSN.databaseName)
	case DbTypeOracle:
		masterDSNURL = fmt.Sprintf("%s/%s@//%s:%d/%s", conDSN.userName, conDSN.password, conDSN.databaseHost, conDSN.port, conDSN.databaseName)
	}

	fmt.Println("\n ---------------------<", "database", ">---------------------")
	fmt.Println("", PadStringToPrintInConsole(strings.ToUpper(conDSN.databaseName), 54, " "))
	fmt.Println("", PadStringToPrintInConsole("------[ Creating connection pool... ]------", 54, " "))
	fmt.Println(" Database Server   : ", conDSN.databaseServer)
	fmt.Println(" Database Host : ", conDSN.databaseHost, conDSN.port)
	//fmt.Println(" Connection URL:", masterDSNURLMasked)

	masterDb, err := sql.Open(string(conDSN.databaseServer), masterDSNURL)
	defer masterDb.Close()

	if err != nil {
		return err
	}

	err = masterDb.Ping()
	if err != nil {
		return err
	}

	//masterDb.SetConnMaxLifetime(time.Minute * 3)
	masterDb.SetMaxIdleConns(conDSN.maxIdleConnections)
	masterDb.SetMaxOpenConns(conDSN.maxOpenConnections)

	connectionsDSNs.conDSNs[organizationId] = conDSN
	return nil
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
