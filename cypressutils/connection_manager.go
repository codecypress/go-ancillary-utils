package cypressutils

import (
	"database/sql"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	cErrors "github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"strings"
)

type DbTypes string

const (
	MySQL        DbTypes = "mysql"
	MicrosoftSQL DbTypes = "sqlserver"
	PostgreSQL   DbTypes = "postgres"
	Oracle       DbTypes = "ora"
)

// DBConDSN Data Source Name
type DBConDSN struct {
	OrganizationId                                                       string
	DatabaseServer                                                       DbTypes
	DatabaseName, DatabaseHost, UserName, Password, ConnectionParameters string
	Port                                                                 int
	MaxIdleConnections, MaxOpenConnections                               int
}

type ConnectionsDSNs struct {
	conDSNs map[string]*DBConDSN
}

var connectionsDSNs *ConnectionsDSNs = &ConnectionsDSNs{
	conDSNs: make(map[string]*DBConDSN),
}

func SetupDSNs() {
	databaseServer := PostgreSQL
	masterUserName, _ := ConfGetDBUserName()
	masterPassword, _ := ConfGetDBPassword()
	masterDatabaseName, _ := ConfGetDatabaseName()
	masterDatabaseHost, _ := ConfGetDBHost()
	masterPort, _ := ConfGetDBPort()
	maxIdleConns, _ := ConfSlingRingInitialPoolSize()
	maxOpenConns, _ := ConfSlingRingMaxPoolSize()

	masterDSNURL := fmt.Sprintf("host=%s Port=%d user=%s Password=%s dbname=%s sslmode=disable",
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

	masterConDSN := &DBConDSN{
		OrganizationId:       "-1L",
		DatabaseServer:       databaseServer,
		DatabaseName:         masterDatabaseName,
		DatabaseHost:         masterDatabaseHost,
		UserName:             masterUserName,
		Password:             masterPassword,
		ConnectionParameters: "",
		Port:                 masterPort,
	}

	connectionsDSNs.conDSNs["-1L"] = masterConDSN

	//TODO: FETCH CONNECTIONS AND POPULATE HERE
}

func SetupDSN(organizationId string, conDSN *DBConDSN) error {

	var masterDSNURL = ""

	switch conDSN.DatabaseServer {
	case PostgreSQL:
		masterDSNURL = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			conDSN.DatabaseHost, conDSN.Port, conDSN.UserName, conDSN.Password, conDSN.DatabaseName)
	case MicrosoftSQL:
		masterDSNURL = fmt.Sprintf("server=%s;user id=%s;Password=%s;Port=%d;database=%s",
			conDSN.DatabaseHost, conDSN.UserName, conDSN.Password, conDSN.Port, conDSN.DatabaseName)
	case MySQL:
		masterDSNURL = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", conDSN.UserName, conDSN.Password, conDSN.DatabaseHost, conDSN.Port, conDSN.DatabaseName)
	case Oracle:
		masterDSNURL = fmt.Sprintf("%s/%s@//%s:%d/%s", conDSN.UserName, conDSN.Password, conDSN.DatabaseHost, conDSN.Port, conDSN.DatabaseName)
	}

	fmt.Println("\n ---------------------<", "database", ">---------------------")
	fmt.Println("", PadStringToPrintInConsole(strings.ToUpper(conDSN.DatabaseName), 54, " "))
	fmt.Println("", PadStringToPrintInConsole("------[ Creating connection pool... ]------", 54, " "))
	fmt.Println(" Database Server   : ", conDSN.DatabaseServer)
	fmt.Println(" Database Host : ", conDSN.DatabaseHost, conDSN.Port)
	//fmt.Println(" Connection URL:", masterDSNURLMasked)

	masterDb, err := sql.Open(string(conDSN.DatabaseServer), masterDSNURL)
	if err != nil {
		ThrowException(err)
		return err
	}

	defer masterDb.Close()

	err = masterDb.Ping()
	if err != nil {
		ThrowException(err)
		return err
	}

	//masterDb.SetConnMaxLifetime(time.Minute * 3)
	masterDb.SetMaxIdleConns(conDSN.MaxIdleConnections)
	masterDb.SetMaxOpenConns(conDSN.MaxOpenConnections)

	connectionsDSNs.conDSNs[organizationId] = conDSN
	return nil
}

func connectToDatabase(databaseType DbTypes, databaseName, databaseHost, userName, password, connectionMetadata string, port int) (*sql.DB, error) {

	masterDSNURL := fmt.Sprintf("host=%s Port=%d user=%s Password=%s dbname=%s sslmode=disable",
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

	var masterDSNURL = ""

	switch conDSN.DatabaseServer {
	case PostgreSQL:
		masterDSNURL = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			conDSN.DatabaseHost, conDSN.Port, conDSN.UserName, conDSN.Password, conDSN.DatabaseName)
	case MicrosoftSQL:
		masterDSNURL = fmt.Sprintf("server=%s;user id=%s;Password=%s;Port=%d;database=%s",
			conDSN.DatabaseHost, conDSN.UserName, conDSN.Password, conDSN.Port, conDSN.DatabaseName)
	case MySQL:
		masterDSNURL = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", conDSN.UserName, conDSN.Password, conDSN.DatabaseHost, conDSN.Port, conDSN.DatabaseName)
	case Oracle:
		masterDSNURL = fmt.Sprintf("%s/%s@//%s:%d/%s", conDSN.UserName, conDSN.Password, conDSN.DatabaseHost, conDSN.Port, conDSN.DatabaseName)
	}

	return sql.Open(string(conDSN.DatabaseServer), masterDSNURL)
}

func GetConDSN(organizationId string) *DBConDSN {
	conDSN, exists := connectionsDSNs.conDSNs[organizationId]
	if !exists {
		err := cErrors.New("No Connection DSN Found where Organization Id = '" + organizationId + "'")
		logrus.Error(err.Error())
		return nil
	}
	return conDSN
}

func (conDSN *DBConDSN) GetOrganizationId() string {
	return conDSN.OrganizationId
}

func (conDSN *DBConDSN) GetDatabaseName() string {
	return conDSN.DatabaseName
}

func (conDSN *DBConDSN) GetDatabaseHost() string {
	return conDSN.DatabaseHost
}

func (conDSN *DBConDSN) GetUsername() string {
	return conDSN.UserName
}

func (conDSN *DBConDSN) GetPassword() string {
	return conDSN.Password
}

func (conDSN *DBConDSN) GetConnectionMetadata() string {
	return conDSN.ConnectionParameters
}

func (conDSN *DBConDSN) GetDatabaseServer() DbTypes {
	return conDSN.DatabaseServer
}

func (conDSN *DBConDSN) GetPort() int {
	return conDSN.Port
}
